package org.hammerlab.spear

import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.RDDInfo
import org.hammerlab.spear.SparkTypedefs.{TaskIndex, RDDID, JobID, TaskID, ExecutorID, StageAttemptID, StageID, Time}
import org.apache.spark.executor.{
  TaskMetrics => SparkTaskMetrics
}

trait DBHelpers extends HasDatabaseService {

  def appId: String

  def getJob(id: JobID) =
    Q(Job)
      .where(_.appId eqs appId)
      .and(_.id eqs id)

  def getStage(id: StageID, attempt: StageAttemptID) =
    Q(Stage)
      .where(_.appId eqs appId)
      .and(_.id eqs id)
      .and(_.attempt eqs attempt)

  def getStageAttempts(id: StageID) =
    Q(Stage)
      .where(_.appId eqs appId)
      .and(_.id eqs id)

  def getStages(jobId: JobID) =
    Q(Stage)
      .where(_.appId eqs appId)
      .and(_.jobId eqs jobId)

  def getNotStartedStages(jobId: JobID) = getStages(jobId).and(_.started neqs true)

  def getStageJobJoin(stageId: StageID) =
    Q(StageJobJoin)
      .where(_.appId eqs appId)
      .and(_.stageId eqs stageId)

  def getExecutor(id: ExecutorID) =
    Q(Executor)
      .where(_.appId eqs appId)
      .and(_.id eqs id)

  def getTask(id: TaskID) =
    Q(Task)
      .where(_.appId eqs appId)
      .and(_.id eqs id)

  def getTaskAttempts(stageId: StageID, attemptId: StageAttemptID, index: TaskIndex) =
    Q(Task)
      .where(_.appId eqs appId)
      .and(_.stageId eqs stageId)
      .and(_.stageAttemptId eqs attemptId)
      .and(_.index eqs index)

  def getTasks(ids: Seq[TaskID]) =
    Q(Task)
      .where(_.appId eqs appId)
      .and(_.id in ids)

  def getRDD(id: RDDID) =
    Q(RDD)
      .where(_.appId eqs appId)
      .and(_.id eqs id)

  def getStageMetrics(id: StageID, attempt: StageAttemptID): (TaskMetrics, TaskMetrics) = {
      db.fetchOne(
        getStage(id, attempt).select(_.metrics, _.validatedMetrics)
      ) match {
        case Some((metricsOpt, validatedMetricsOpt)) =>
          (
            metricsOpt.getOrElse(TaskMetrics.newBuilder.result),
            validatedMetricsOpt.getOrElse(TaskMetrics.newBuilder.result)
          )
        case None => (TaskMetrics.newBuilder.result, TaskMetrics.newBuilder.result)
      }
  }

  def getExecutorMetrics(id: ExecutorID): (TaskMetrics, TaskMetrics) = {
    db.fetchOne(
      getExecutor(id).select(_.metrics, _.validatedMetrics)
    ) match {
      case Some((metricsOpt, validatedMetricsOpt)) =>
        (
          metricsOpt.getOrElse(TaskMetrics.newBuilder.result),
          validatedMetricsOpt.getOrElse(TaskMetrics.newBuilder.result)
          )
      case None => (TaskMetrics.newBuilder.result, TaskMetrics.newBuilder.result)
    }
  }

  def getTaskMetricsDeltasMap(metrics: Seq[(TaskID, _, _, SparkTaskMetrics)]): Map[TaskID, TaskMetrics] = {
    val taskIds = metrics.map(_._1)

    val fromDB = db.fetch(
      getTasks(taskIds).select(_.id, _.metrics.slice(-1))
    )
    val existingTaskMetrics: Map[TaskID, TaskMetrics] =
      fromDB.flatMap {
        case (Some(id), Some(Seq(metrics))) => Some(id, metrics)
        case (Some(_), None) | (Some(_), Some(Seq())) => Map.empty
        case x =>
          throw new Exception(
            s"Unknown result fetching existing metrics for tasks ${taskIds.mkString(",")}: $x"
          )
      }.toMap

    (for {
      (taskId, _, _, sparkMetrics) <- metrics
      newMetrics = SparkIDL.taskMetrics(sparkMetrics)
      existingMetricsOpt = existingTaskMetrics.get(taskId)
      deltaMetrics = SparkIDL.combineMetrics(newMetrics, existingMetricsOpt, add = false)
    } yield {
        taskId -> deltaMetrics
      }).toMap
  }

  def computeNewMetrics(existingMetrics: (TaskMetrics, TaskMetrics),
                        deltas: Seq[(TaskID, TaskMetrics)],
                        tasksAlreadyExisted: Set[TaskID]): (TaskMetrics, TaskMetrics) = {
    deltas.foldLeft(existingMetrics)((e,p) => {
      val (existing, validated) = e
      val (tid, metrics) = p
      (
        SparkIDL.combineMetrics(existing, Some(metrics), add = true),
        if (tasksAlreadyExisted(tid))
          SparkIDL.combineMetrics(validated, Some(metrics), add = true)
        else
          validated
        )
    })
  }

  def updateStageMetrics(metrics: Seq[(TaskID, StageID, StageAttemptID, SparkTaskMetrics)],
                         metricsDeltas: Map[TaskID, TaskMetrics],
                         tasksAlreadyExisted: Set[TaskID]) = {

    val stagesToTaskIDs: Map[(StageID, StageAttemptID), Seq[TaskID]] =
      metrics.map {
        case (taskId, stageId, stageAttempt, taskMetrics) =>
          (stageId, stageAttempt) -> taskId
      }.groupBy(_._1).mapValues(_.map(_._2))

    for {
      ((stageId, stageAttempt), taskIDs) <- stagesToTaskIDs
    } {
      val stageDeltas = for {
        id <- taskIDs
        delta <- metricsDeltas.get(id)
      } yield {
          (id, delta)
      }

      val (newMetrics, newValidatedMetrics) =
        computeNewMetrics(
          getStageMetrics(stageId, stageAttempt),
          stageDeltas,
          tasksAlreadyExisted
        )

      db.findAndUpdateOne(
        getStage(stageId, stageAttempt)
          .findAndModify(_.metrics setTo newMetrics)
          .and(_.validatedMetrics setTo newValidatedMetrics)
      )
    }
  }

  def updateExecutorMetrics(execID: ExecutorID,
                            metricsDeltas: Map[TaskID, TaskMetrics],
                            tasksAlreadyExisted: Set[TaskID]) = {

    val (newMetrics, newValidatedMetrics) =
      computeNewMetrics(
        getExecutorMetrics(execID),
        metricsDeltas.toSeq,
        tasksAlreadyExisted
      )

    db.findAndUpdateOne(
      getExecutor(execID)
        .findAndModify(_.metrics setTo newMetrics)
        .and(_.validatedMetrics setTo newValidatedMetrics)
    )
  }

  def upsertRDD(ri: RDDInfo): Unit = {
    db.findAndUpsertOne(
      getRDD(ri.id)
        .findAndModify(_.name setTo ri.name)
        .and(_.numPartitions setTo ri.numPartitions)
        .and(_.storageLevel setTo SparkIDL.storageLevel(ri.storageLevel))
        .and(_.numCachedPartitions setTo ri.numCachedPartitions)
        .and(_.memSize setTo ri.memSize)
        .and(_.diskSize setTo ri.diskSize)
        .and(_.tachyonSize setTo ri.tachyonSize)
    )
  }

  def upsertRDDs(si: StageInfo): Unit = {
    si.rddInfos.foreach(upsertRDD)
  }

  def upsertRDDs(rddInfos: Seq[RDDInfo]): Unit = {
    rddInfos.foreach(upsertRDD)
  }

  def makeDuration(start: Time): Duration = {
    Duration.newBuilder.start(start).result
  }

  def makeDuration(start: Time, end: Time): Duration = {
    Duration.newBuilder.start(start).end(end).result
  }

  def makeDuration(startOpt: Option[Time], endOpt: Option[Time] = None): Option[Duration] = {
    (startOpt, endOpt) match {
      case (None, None) => None
      case _ => Some(Duration.newBuilder.start(startOpt).end(endOpt).result)
    }
  }
}
