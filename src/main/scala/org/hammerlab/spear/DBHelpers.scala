package org.hammerlab.spear

import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.RDDInfo
import org.hammerlab.spear.SparkTypedefs.{TaskID, ExecutorID, StageAttemptID, StageID, Time}
import org.apache.spark.executor.{
  TaskMetrics => SparkTaskMetrics
}

trait DBHelpers extends HasDatabaseService {

  def getStageMetrics(id: StageID, attempt: StageAttemptID): TaskMetrics = {
    db.fetchOne(
      Q(Stage)
      .where(_.id eqs id)
      .and(_.attempt eqs attempt)
      .select(_.metrics)
    ).flatten.getOrElse(TaskMetrics.newBuilder.result)
  }

  def getExecutorMetrics(id: ExecutorID): TaskMetrics = {
    db.fetchOne(
      Q(Executor)
      .where(_.id eqs id)
      .select(_.metrics)
    ).flatten.getOrElse(TaskMetrics.newBuilder.result)
  }

  def getTaskMetricsDeltasMap(metrics: Seq[(TaskID, _, _, SparkTaskMetrics)]): Map[TaskID, TaskMetrics] = {
    val taskIds = metrics.map(_._1)

    val existingTaskMetrics: Map[TaskID, TaskMetrics] =
      db.fetch(
        Q(Task).where(_.id in taskIds).select(_.id, _.metrics.slice(-1))
      ).flatMap {
        case (Some(id), Some(metrics :: Nil)) => Some(id, metrics)
        case _ => None
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

  def updateStageMetrics(metrics: Seq[(TaskID, StageID, StageAttemptID, SparkTaskMetrics)],
                         metricsDeltas: Map[TaskID, TaskMetrics]) = {

    val stagesToTaskIDs: Map[(StageID, StageAttemptID), Seq[TaskID]] =
      metrics.map {
        case (taskId, stageId, stageAttempt, taskMetrics) =>
          (stageId, stageAttempt) -> taskId
      }.groupBy(_._1).mapValues(_.map(_._2))

    for {
      ((stageId, stageAttempt), taskIDs) <- stagesToTaskIDs
    } {
      val existingMetrics = getStageMetrics(stageId, stageAttempt)

      val newMetrics =
        (for {
          id <- taskIDs
          delta <- metricsDeltas.get(id)
        } yield {
            delta
          }).foldLeft(existingMetrics)((e,m) => {
          SparkIDL.combineMetrics(e, Some(m), add = true)
        })

      db.findAndUpdateOne(
        Q(Stage)
        .where(_.id eqs stageId)
        .and(_.attempt eqs stageAttempt)
        .findAndModify(_.metrics setTo newMetrics)
      )
    }
  }

  def updateExecutorMetrics(execID: ExecutorID,
                            metrics: Seq[(TaskID, StageID, StageAttemptID, SparkTaskMetrics)],
                            metricsDeltas: Map[TaskID, TaskMetrics]) = {

    val existingExecutorMetrics = getExecutorMetrics(execID)

    val newExecutorMetrics = metricsDeltas.values.toList.foldLeft(existingExecutorMetrics)((e,m) => {
      SparkIDL.combineMetrics(e, Some(m), add = true)
    })

    db.findAndUpdateOne(
      Q(Executor)
      .where(_.id eqs execID)
      .findAndModify(_.metrics setTo newExecutorMetrics)
    )
  }

  def upsertRDD(ri: RDDInfo): Unit = {
    db.findAndUpsertOne(
      Q(RDD)
      .where(_.id eqs ri.id)
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
