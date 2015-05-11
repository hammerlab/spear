
package org.hammerlab.spear

import com.mongodb.casbah.{MongoCollection, MongoClient}
import com.mongodb.casbah.Implicits._

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.scheduler.{
    StageInfo => SparkStageInfo,
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
import org.hammerlab.spear.SparkIDL.{ExecutorID, JobID, StageID, RDDID}


case class AccumulableInfo(id: Long,
                           name: String,
                           update: Option[String], // represents a partial update within a task
                           value: String)

case class Job(id: JobID,
               time: Long,
               stageIDs: Seq[StageID],
               finishTime: Option[Long] = None,
               succeeded: Option[Boolean] = None)

case class Stage(id: StageID,
                 attempt: Int,
                 name: String,
                 numTasks: Int,
                 rdds: Seq[RDDID],
                 details: String,
                 tasksStarted: Option[Int] = None,
                 tasksSucceeded: Option[Int] = None,
                 tasksFailed: Option[Int] = None,
                 submissionTime: Option[Long] = None,
                 completionTime: Option[Long] = None,
                 failureReason: Option[String] = None,
                 accumulables: Option[Map[Long, AccumulableInfo]] = None)  // TODO(ryan): implement

case class RDD(id: RDDID,
               name: String,
               numPartitions: Int,
               storageLevel: Int)  // TODO(ryan): enums

case class Executor(id: ExecutorID,
                    host: String,
                    //port: Int,
                    totalCores: Option[Int] = None,
                    removedAt: Option[Long] = None,
                    removedReason: Option[String] = None,
                    logUrlMap: Option[Map[String, String]] = None)

case class Task(id: Long,
                index: Int,
                attempt: Int,
                stageId: StageID,
                stageAttemptId: Int,
                launchTime: Long,
                execId: ExecutorID,
                taskLocality: Int,
                speculative: Option[Boolean] = None,
                gettingResult: Option[Boolean] = None,
                taskType: Option[String] = None,
                taskEndReason: Option[TaskEndReason] = None,
                metrics: Option[TaskMetrics] = None)

class Spear(sc: SparkContext,
            mongoHost: String = "localhost",
            mongoPort: Int = 27017) extends SparkListener {

  val applicationId = sc.applicationId

  val client = MongoClient(mongoHost, mongoPort)

  println(s"Creating database for appplication: $applicationId")
  val db = client(applicationId)

  val jobs = db("jobs")
  jobs.ensureIndex(Map("id" -> 1))

  val stages = db("stages")
  stages.ensureIndex(Map("id" -> 1, "attempt" -> 1))

  val tasks = db("tasks")
  tasks.ensureIndex(Map("stageId" -> 1, "stageAttemptId" -> 1, "id" -> 1, "attempt" -> 1))
  tasks.ensureIndex(Map("index" -> 1))

  val taskMetricsColl = db("metrics")
  taskMetricsColl.ensureIndex(Map("stageId" -> 1, "stageAttemptId" -> 1, "id" -> 1))

  val executors = db("executors")
  executors.ensureIndex(Map("id" -> 1))
  executors.ensureIndex(Map("host" -> 1))

  val rdds = db("rdds")
  rdds.ensureIndex(Map("id" -> 1))

  // Collections
  val submittedStages = db("stage_starts")
  val completedStages = db("stage_ends")

  val startedTasks = db("task_starts")
  val taskGettingResults = db("task_getting_results")
  val taskEnds = db("task_ends")

  val jobStarts = db("job_starts")
  val jobEnds = db("job_ends")

  val envUpdates = db("env_updates")

  val blockManagerAdds = db("block_manager_adds")
  val blockManagerRemoves = db("block_manager_removes")

  val rddUnpersists = db("rdd_unpersists")

  val appStarts = db("app_starts")
  val appEnds = db("app_ends")

  val executorMetricUpdates = db("executor_updates")
  val executorAdds = db("executor_adds")
  val executorRemoves = db("executor_removes")

  sc.addSparkListener(this)

  // Add executors
  executors.insert(
    SparkEnv.get.blockManager.master.getMemoryStatus.keySet.toList.map(b =>
     MongoCaseClassSerializer.to(
       Executor(
         b.executorId,
         s"${b.host}:${b.port}"
       )
     )
    ):_*
  )
  def serializeAndInsert[T <: AnyRef](t: T, collection: MongoCollection)(implicit m: Manifest[T]): Unit = {
    collection.insert(MongoCaseClassSerializer.to(t))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    upsertStage(stageCompleted.stageInfo)
  }

  def upsertStage(si: SparkStageInfo): Unit = {
    stages.update(
      Map("id" -> si.stageId, "attempt" -> si.attemptId),
      Map("$set" ->
        MongoCaseClassSerializer.to(
          Stage(
            si.stageId,
            si.attemptId,
            si.name,
            si.numTasks,
            si.rddInfos.map(_.id),
            si.details,
            submissionTime = si.submissionTime,
            completionTime = si.completionTime,
            failureReason = si.failureReason
          )
        )
      ),
      upsert = true
    )
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    upsertStage(stageSubmitted.stageInfo)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val ti = taskStart.taskInfo
    tasks.insert(
      MongoCaseClassSerializer.to(
        Task(
          ti.taskId,
          ti.index,
          ti.attempt,
          taskStart.stageId,
          taskStart.stageAttemptId,
          ti.launchTime,
          ti.executorId,
          ti.taskLocality.id,
          speculative = if (ti.speculative) Some(ti.speculative) else None
        )
      )
    )

    stages.update(
      Map("id" -> taskStart.stageId, "attempt" -> taskStart.stageAttemptId),
      Map("$inc" -> Map("tasksStarted" -> 1))
    )
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    tasks.update(
      Map(
        "index" -> taskGettingResult.taskInfo.index
      ),
      Map("$set" ->
        Map(
          "gettingResult" -> true
        )
      )
    )
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val reason = TaskEndReason(taskEnd.reason)
    val reasonObj = MongoCaseClassSerializer.to(reason)
    val success = reason.success.exists(x => x)
    serializeAndInsert(TaskEndEvent(taskEnd), taskEnds)
    val keys = Map(
      "stageId" -> taskEnd.stageId,
      "stageAttemptId" -> taskEnd.stageAttemptId,
      "id" -> taskEnd.taskInfo.taskId,
      "attempt" -> taskEnd.taskInfo.attempt
    )
    val tm = MongoCaseClassSerializer.to(TaskMetrics(taskEnd.taskMetrics))
    val wr =
      tasks.update(
        keys,
        Map("$set" ->
          Map(
            "taskType" -> taskEnd.taskType,
            "taskEndReason" -> reasonObj,
            "metrics" -> tm
          )
        )
      )

    if (!wr.isUpdateOfExisting) {
      System.err.println(s"ERROR: no task updated with keys: $keys")
    }

    stages.update(
      Map(
        "id" -> taskEnd.stageId,
        "attempt" -> taskEnd.stageAttemptId
      ),
      Map("$inc" ->
        Map(
          (if (success) "tasksSucceeded" else "tasksFailed") -> 1
        )
      )
    )

    taskMetricsColl.update(
      Map(
        "stageId" -> taskEnd.stageId,
        "stageAttemptId" -> taskEnd.stageAttemptId,
        "id" -> taskEnd.taskInfo.taskId
      ),
      Map(
        "$push" -> Map(
          "metrics" -> tm
        )
      ),
      upsert = true
    )
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    serializeAndInsert(Job(jobStart.jobId, jobStart.time, jobStart.stageIds), jobs)
    jobStart.stageInfos.map(upsertStage)

    jobStart.stageInfos.flatMap(_.rddInfos).map(rddInfo => {
      rdds.update(
        Map("id" -> rddInfo.id),
        Map(
          "$set" ->
            MongoCaseClassSerializer.to(
              RDD(
                rddInfo.id,
                rddInfo.name,
                rddInfo.numPartitions,
                rddInfo.storageLevel.toInt
              )
            )
        ),
        upsert = true
      )
    })
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobs.update(
      Map("id" -> jobEnd.jobId),
      Map("$set" ->
        Map(
          "finishTime" -> Some(jobEnd.time),
          "succeeded" -> Some(jobEnd.jobResult == JobSucceeded)
        )
      )
    )
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    serializeAndInsert(environmentUpdate, envUpdates)
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    serializeAndInsert(BlockManagerAddedEvent(blockManagerAdded), blockManagerAdds)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    serializeAndInsert(BlockManagerRemovedEvent(blockManagerRemoved), blockManagerRemoves)
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    serializeAndInsert(unpersistRDD, rddUnpersists)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    serializeAndInsert(applicationStart, appStarts)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    serializeAndInsert(applicationEnd, appEnds)
  }

  def parseTaskId(taskId: String): Option[(Int, Int)] = {
    taskId.split('.') match {
      case Array(id, attempt) =>
        try {
          Some((id.toInt, attempt.toInt))
        } catch {
          case _ => None
        }
      case _ => None
    }
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    if (executorMetricsUpdate.taskMetrics.size > 0) {
      executorMetricsUpdate.taskMetrics.map {
        case (taskId, stageId, stageAttempt, taskMetrics) =>
          taskMetricsColl.update(
            Map(
              "stageId" -> stageId,
              "stageAttemptId" -> stageAttempt,
              "id" -> taskId
            ),
            Map(
              "$push" -> Map(
                "metrics" -> MongoCaseClassSerializer.to(
                  TaskMetrics(
                    taskMetrics
                  )
                )
              )
            ),
            upsert = true
          )
      }
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val ei = executorAdded.executorInfo
    executors.insert(
      MongoCaseClassSerializer.to(
        Executor(
          executorAdded.executorId,
          ei.executorHost,
          Some(ei.totalCores),
          logUrlMap = if (ei.logUrlMap.nonEmpty) Some(ei.logUrlMap) else None
        )
      )
    )
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    executors.update(
      Map("id" -> executorRemoved.executorId),
      Map("$set" ->
        Map(
          "removedAt" -> executorRemoved.time,
          "removedReason" -> executorRemoved.reason
        )
      )
    )
  }
}
