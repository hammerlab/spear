
package org.hammerlab.spear

import com.foursquare.rogue.spindle.{SpindleDBCollectionFactory, SpindleDatabaseService}
import com.foursquare.spindle.UntypedMetaRecord
import com.mongodb.{DB, MongoClient}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.scheduler.{
    JobSucceeded,
    SparkListenerExecutorRemoved,
    SparkListenerExecutorAdded,
    SparkListenerExecutorMetricsUpdate,
    SparkListenerApplicationEnd,
    SparkListenerApplicationStart,
    SparkListenerUnpersistRDD,
    SparkListenerBlockManagerRemoved,
    SparkListenerBlockManagerAdded,
    SparkListenerEnvironmentUpdate,
    SparkListenerJobEnd,
    SparkListenerJobStart,
    SparkListenerTaskGettingResult,
    SparkListenerStageSubmitted,
    SparkListenerStageCompleted,
    SparkListener,
    SparkListenerTaskEnd,
    SparkListenerTaskStart
}
import org.hammerlab.spear.SparkTypedefs.TaskID
import org.hammerlab.spear.TaskEndReasonType.SUCCESS

class Spear(sc: SparkContext,
            mongoHost: String = "localhost",
            mongoPort: Int = 27017) extends SparkListener {

  val applicationId = sc.applicationId

  object db extends SpindleDatabaseService(ConcreteDBCollectionFactory)

  object ConcreteDBCollectionFactory extends SpindleDBCollectionFactory {
    lazy val db: DB = new MongoClient(mongoHost, mongoPort).getDB(applicationId)
    override def getPrimaryDB(meta: UntypedMetaRecord) = db
    override def indexCache = None
  }


  println(s"Creating database for appplication: $applicationId")

  sc.addSparkListener(this)

  // Add executors
  db.insertAll(
    SparkEnv.get.blockManager.master.getMemoryStatus.keySet.toList.map(b =>
        Executor.newBuilder.id(b.executorId).host(b.host).port(b.port).result()
    )
  )

  // Stage events
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val si = stageSubmitted.stageInfo
    db.findAndUpsertOne(
      Q(Stage)
        .where(_.id eqs si.stageId)
        .and(_.attempt eqs si.attemptId)
        .findAndModify(_.name setTo si.name)
        .and(_.numTasks setTo si.numTasks)
        .and(_.rddIDs setTo si.rddInfos.map(_.id))
        .and(_.details setTo si.details)
        .and(_.startTime setTo si.submissionTime)
        .and(_.endTime setTo si.completionTime)
        .and(_.failureReason setTo si.failureReason)
        .and(_.properties setTo SparkIDL.properties(stageSubmitted.properties))
    )

    // TODO(ryan): verify whether this will lead to duplicate RDD records, e.g.
    // if stages share an RDD; switch to upserting RDD records one by one
    // instead if so.
    db.insertAll(
      si.rddInfos.map(ri => {
        RDD.newBuilder
          .id(ri.id)
          .name(ri.name)
          .numPartitions(ri.numPartitions)
          .storageLevel(SparkIDL.storageLevel(ri.storageLevel))
          .numCachedPartitions(ri.numCachedPartitions)
          .memSize(ri.memSize)
          .diskSize(ri.diskSize)
          .tachyonSize(ri.tachyonSize)
          .result()
      })
    )
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val si = stageCompleted.stageInfo
    db.findAndUpdateOne(
      Q(Stage)
        .where(_.id eqs si.stageId)
        .and(_.attempt eqs si.attemptId)
        .findAndModify(_.endTime setTo si.completionTime)
        // submissionTime sometimes doesn't make it into the StageSubmitted
        // event, likely due to a race on the Spark side.
        .and(_.startTime setTo si.submissionTime)
        .and(_.failureReason setTo si.failureReason)
    )
  }

  // Task events
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val ti = taskStart.taskInfo
    db.insert(
      Task.newBuilder
        .id(ti.taskId)
        .index(ti.index)
        .attempt(ti.attempt)
        .stageId(taskStart.stageId)
        .stageAttemptId(taskStart.stageAttemptId)
        .startTime(ti.launchTime)
        .execId(ti.executorId)
        .taskLocality(TaskLocality.findById(ti.taskLocality.id))
        .speculative(ti.speculative)
        .result()
    )

    val q = Q(Stage)
            .where(_.id eqs taskStart.stageId)
            .and(_.attempt eqs taskStart.stageAttemptId)
            .findAndModify(_.tasksStarted inc 1)

    db.findAndUpdateOne(q)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    db.findAndUpdateOne(
      Q(Task)
        .where(_.id eqs taskGettingResult.taskInfo.taskId)
        .findAndModify(_.gettingResult setTo true)
    )
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val reason = SparkIDL.taskEndReason(taskEnd.reason)
    val success = reason.tpeOption().exists(_ == SUCCESS)
    val tm = SparkIDL.taskMetrics(taskEnd.taskMetrics)
    db.findAndUpdateOne(
      Q(Task)
        .where(_.id eqs taskEnd.taskInfo.taskId)
        .findAndModify(_.taskType setTo taskEnd.taskType)
        .and(_.taskEndReason setTo reason)
        .and(_.metrics push tm)
    )

    db.findAndUpdateOne(
      Q(Stage)
        .where(_.id eqs taskEnd.stageId)
        .and(_.attempt eqs taskEnd.stageAttemptId)
        .findAndModify(s => (
            if (success) s.tasksSucceeded
            else s.tasksFailed
          ) inc 1
        )
    )
  }

  // Job events
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    val job = Job.newBuilder
              .id(jobStart.jobId)
              .startTime(jobStart.time)
              .stageIDs(jobStart.stageIds)
              .properties(SparkIDL.properties(jobStart.properties))
              .result()
    db.insert(job)

    val stages = jobStart.stageInfos.map(si => {
      Stage.newBuilder
        .id(si.stageId)
        .attempt(si.attemptId)
        .name(si.name)
        .numTasks(si.numTasks)
        .rddIDs(si.rddInfos.map(_.id))
        .details(si.details)
        .startTime(si.submissionTime)
        .result()
    })

    // NOTE(ryan): assumes that this happens before the StageSubmitted event,
    // otherwise we'd get duplicate Stage records.
    db.insertAll(stages)

    val rdds = jobStart.stageInfos.flatMap(_.rddInfos).map(ri => {
      RDD.newBuilder
        .id(ri.id)
        .name(ri.name)
        .numPartitions(ri.numPartitions)
        .storageLevel(SparkIDL.storageLevel(ri.storageLevel))
        .result()
    })

    db.insertAll(rdds)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    db.findAndUpdateOne(
      Q(Job)
        .where(_.id eqs jobEnd.jobId)
        .findAndModify(_.endTime setTo jobEnd.time)
        .and(_.succeeded setTo (jobEnd.jobResult == JobSucceeded))
    )
  }

  // Executor events
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {

    if (executorMetricsUpdate.taskMetrics.size > 0) {

      // Update Task records
      executorMetricsUpdate.taskMetrics.map {
        case (taskId, stageId, stageAttempt, taskMetrics) =>
          db.findAndUpdateOne(
            Q(Task)
            .where(_.id eqs taskId)
            .findAndModify(_.metrics push SparkIDL.taskMetrics(taskMetrics))
          )
      }

      val taskIds = executorMetricsUpdate.taskMetrics.map(_._1)

      val metrics = db.fetch(
        Q(Task).where(_.id in taskIds).select(_.id, _.metrics.slice(-1))
      )

      val existingTasks: Map[TaskID, TaskMetrics] =
        metrics.flatMap {
          case (Some(id), Some(metrics :: Nil)) => Some(id, metrics)
          case _ => None
        }.toMap

      val currentMetrics: Map[TaskID, TaskMetrics] =
        executorMetricsUpdate.taskMetrics.map {
          case (taskId, stageId, stageAttempt, taskMetrics) =>
            taskId -> SparkIDL.taskMetrics(taskMetrics)
        }.toMap

      val metricsDeltas: Map[TaskID, TaskMetrics] =
        (for {
          (taskID, metrics) <- currentMetrics
          existing = existingTasks.get(taskID)
        } yield {
            taskID -> SparkIDL.combineMetrics(metrics, existing, add = false)
          }).toMap

      // Update Executor metrics
      val existingExecutorMetrics =
        db.fetchOne(
          Q(Executor)
            .where(_.id eqs executorMetricsUpdate.execId)
            .select(_.metrics)
        ).flatten.getOrElse(TaskMetrics.newBuilder.result)

      val newExecutorMetrics = metricsDeltas.values.toList.foldLeft(existingExecutorMetrics)((e,m) => {
        SparkIDL.combineMetrics(e, Some(m), add = true)
      })

      db.findAndUpdateOne(
        Q(Executor)
          .where(_.id eqs executorMetricsUpdate.execId)
          .findAndModify(_.metrics setTo newExecutorMetrics)
      )

      // Update Stage metrics
      val stagesToTaskIDs =
        executorMetricsUpdate.taskMetrics.map {
          case (taskId, stageId, stageAttempt, taskMetrics) =>
            (stageId, stageAttempt) -> taskId
        }.groupBy(_._1).mapValues(_.map(_._2))

      for {
        ((stageId, stageAttempt), taskIDs) <- stagesToTaskIDs
      } {
        val existingMetrics =
          db.fetchOne(
            Q(Stage)
              .where(_.id eqs stageId)
              .and(_.attempt eqs stageAttempt)
              .select(_.metrics)
          ).flatten.getOrElse(TaskMetrics.newBuilder.result)

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
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

    val (host, portOpt) = executorAdded.executorInfo.executorHost.split(":") match {
      case Array(host, port) => (host, Some(port.toInt))
      case Array(host) => (host, None)
      case _ => throw new Exception(
        s"Malformed executor host string? ${executorAdded.executorInfo.executorHost}"
      )
    }

    db.insert(
      Executor.newBuilder
        .id(executorAdded.executorId)
        .host(host)
        .port(portOpt)
        .addedAt(executorAdded.time)
        .totalCores(executorAdded.executorInfo.totalCores)
        .logUrlMap(executorAdded.executorInfo.logUrlMap)
        .result()
    )
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    db.findAndUpdateOne(
      Q(Executor)
        .where(_.id eqs executorRemoved.executorId)
        .findAndModify(_.removedAt setTo executorRemoved.time)
        .and(_.removedReason setTo executorRemoved.reason)
    )
  }

  // Misc events
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    // TODO(ryan)
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    // TODO(ryan)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    // TODO(ryan)
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    // TODO(ryan)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    // TODO(ryan)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    // TODO(ryan)
  }

}
