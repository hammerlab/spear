
package org.hammerlab.spear

import com.mongodb.casbah.{MongoCollection, MongoClient}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{
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


class Spear(sc: SparkContext,
            mongoHost: String = "localhost",
            mongoPort: Int = 27017) extends SparkListener {

  val applicationId = sc.applicationId

  val client = MongoClient(mongoHost, mongoPort)

  println(s"Creating database for appplication: $applicationId")
  val db = client(applicationId)


  // Collections
  val submittedStages = db("stage_starts")
  val completedStages = db("stage_ends")

  val startedTasks = db("task_starts")
  val taskGettingResults = db("task_getting_results")
  val endedTasks = db("task_ends")

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

  def serializeAndInsert[T <: AnyRef](t: T, collection: MongoCollection)(implicit m: Manifest[T]): Unit = {
    collection.insert(MongoCaseClassSerializer.to(t))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    serializeAndInsert(StageInfo(stageCompleted.stageInfo), completedStages)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    serializeAndInsert(StageInfo(stageSubmitted.stageInfo), submittedStages)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    serializeAndInsert(TaskStartEvent(taskStart), startedTasks)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    serializeAndInsert(TaskInfo(taskGettingResult.taskInfo), taskGettingResults)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    serializeAndInsert(TaskEndEvent(taskEnd), endedTasks)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    serializeAndInsert(JobStartEvent(jobStart), jobStarts)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    serializeAndInsert(JobEndEvent(jobEnd), jobEnds)
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

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    if (executorMetricsUpdate.taskMetrics.size > 0) {
      serializeAndInsert(ExecutorMetricsUpdateEvent(executorMetricsUpdate), executorMetricUpdates)
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    serializeAndInsert(ExecutorAddedEvent(executorAdded), executorAdds)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    serializeAndInsert(executorRemoved, executorRemoves)
  }
}
