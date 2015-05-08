
package org.hammerlab.spear

import java.util.Properties

import com.mongodb.casbah.{MongoCollection, MongoClient}
import scala.collection.JavaConversions._

import org.apache.spark.{
    TaskEndReason => SparkTaskEndReason,
    SparkContext,
    Success,
    UnknownReason,
    TaskKilled,
    TaskResultLost,
    Resubmitted,
    ExecutorLostFailure,
    TaskCommitDenied,
    ExceptionFailure,
    FetchFailed
}
import org.apache.spark.executor.{
    TaskMetrics => SparkTaskMetrics,
    ShuffleWriteMetrics => SparkShuffleWriteMetrics,
    ShuffleReadMetrics => SparkShuffleReadMetrics,
    InputMetrics => SparkInputMetrics,
    OutputMetrics => SparkOutputMetrics
}
import org.apache.spark.storage.{
    RDDInfo => SparkRDDInfo,
    StorageLevel => SparkStorageLevel,
    BlockStatus => SparkBlockStatus,
    BlockManagerId => SparkBlockManagerId,
    BlockId
}
import org.apache.spark.scheduler.{
    StageInfo => SparkStageInfo,
    TaskInfo => SparkTaskInfo,
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
import org.apache.spark.scheduler.cluster.{ExecutorInfo => SparkExecutorInfo}


case class TaskInfo(taskId: Long,
                    index: Int,
                    attempt: Int,
                    launchTime: Long,
                    executorId: String,
                    host: String,
                    taskLocality: Int,
                    speculative: Boolean)

object TaskInfo {
  def apply(t: SparkTaskInfo): TaskInfo =
    new TaskInfo(
      t.taskId,
      t.index,
      t.attempt,
      t.launchTime,
      t.executorId,
      t.host,
      t.taskLocality.id,
      t.speculative
    )
}

case class StorageLevel(n: Int)

object StorageLevel {
  def apply(s: SparkStorageLevel): StorageLevel = new StorageLevel(s.toInt)
}

case class RDDInfo(id: Int,
                   name: String,
                   numPartitions: Int,
                   storageLevel: StorageLevel)

object RDDInfo {
  def apply(r: SparkRDDInfo): RDDInfo =
    new RDDInfo(
      r.id,
      r.name,
      r.numPartitions,
      StorageLevel(r.storageLevel)
    )
}

case class StageInfo(stageId: Int,
                     attemptId: Int,
                     name: String,
                     numTasks: Int,
                     rddInfos: Seq[RDDInfo],
                     details: String)

object StageInfo {
  def apply(s: SparkStageInfo): StageInfo =
    new StageInfo(
      s.stageId,
      s.attemptId,
      s.name,
      s.numTasks,
      s.rddInfos.map(RDDInfo.apply),
      s.details
    )
}

case class InputMetrics(bytesRead: Long,
                        recordsRead: Long)
object InputMetrics {
  def apply(i: SparkInputMetrics): InputMetrics = new InputMetrics(i.bytesRead, i.recordsRead)
}

case class OutputMetrics(bytesWritten: Long,
                         recordsWritten: Long)
object OutputMetrics {
  def apply(o: SparkOutputMetrics): OutputMetrics =
    new OutputMetrics(
      o.bytesWritten,
      o.recordsWritten
    )
}

case class ShuffleReadMetrics(remoteBlocksFetched: Int,
                              localBlocksFetched: Int,
                              fetchWaitTime: Long,
                              remoteBytesRead: Long,
                              localBytesRead: Long,
                              recordsRead: Long)
object ShuffleReadMetrics {
  def apply(s: SparkShuffleReadMetrics): ShuffleReadMetrics =
    new ShuffleReadMetrics(
      s.remoteBlocksFetched,
      s.localBlocksFetched,
      s.fetchWaitTime,
      s.remoteBytesRead,
      s.localBytesRead,
      s.recordsRead
    )
}

case class ShuffleWriteMetrics(shuffleBytesWritten: Long,
                               shuffleWriteTime: Long,
                               shuffleRecordsWritten: Long)
object ShuffleWriteMetrics {
  def apply(s: SparkShuffleWriteMetrics): ShuffleWriteMetrics =
    new ShuffleWriteMetrics(
      s.shuffleBytesWritten,
      s.shuffleWriteTime,
      s.shuffleRecordsWritten
    )
}

case class BlockStatus(storageLevel: StorageLevel,
                       memSize: Long,
                       diskSize: Long,
                       tachyonSize: Long)
object BlockStatus {
  def apply(b: SparkBlockStatus): BlockStatus =
    new BlockStatus(
      StorageLevel(b.storageLevel),
      b.memSize,
      b.diskSize,
      b.tachyonSize
    )
}


case class TaskMetrics(hostname: String,
                       executorDeserializeTime: Long,
                       executorRunTime: Long,
                       resultSize: Long,
                       jvmGCTime: Long,
                       resultSerializationTime: Long,
                       memoryBytesSpilled: Long,
                       diskBytesSpilled: Long,
                       inputMetrics: Option[InputMetrics],
                       outputMetrics: Option[OutputMetrics],
                       shuffleReadMetrics: Option[ShuffleReadMetrics],
                       shuffleWriteMetrics: Option[ShuffleWriteMetrics],
                       updatedBlocks: Option[Seq[(BlockId, BlockStatus)]])

object TaskMetrics {
  def apply(t: SparkTaskMetrics): TaskMetrics =
    new TaskMetrics(
      t.hostname,
      t.executorDeserializeTime,
      t.executorRunTime,
      t.resultSize,
      t.jvmGCTime,
      t.resultSerializationTime,
      t.memoryBytesSpilled,
      t.diskBytesSpilled,
      t.inputMetrics.map(InputMetrics.apply),
      t.outputMetrics.map(OutputMetrics.apply),
      t.shuffleReadMetrics.map(ShuffleReadMetrics.apply),
      t.shuffleWriteMetrics.map(ShuffleWriteMetrics.apply),
      t.updatedBlocks.map(_.map(p => (p._1, BlockStatus(p._2))))
    )
}

case class TaskStartEvent(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)

object TaskStartEvent {
  def apply(s: SparkListenerTaskStart): TaskStartEvent =
    new TaskStartEvent(
      s.stageId,
      s.stageAttemptId,
      TaskInfo(s.taskInfo)
    )
}

case class TaskEndReason(success: Option[Boolean] = None,
                         resubmitted: Option[Boolean] = None,
                         taskResultLost: Option[Boolean] = None,
                         taskKilled: Option[Boolean] = None,
                         unknownReason: Option[Boolean] = None,
                         fetchFailed: Option[FetchFailed] = None,
                         exceptionFailure: Option[ExceptionFailure] = None,
                         taskCommitDenied: Option[TaskCommitDenied] = None,
                         executorLostFailure: Option[ExecutorLostFailure] = None)

object TaskEndReason {
  def apply(r: SparkTaskEndReason): TaskEndReason = r match {
    case Success => TaskEndReason(success = Some(true))
    case Resubmitted => TaskEndReason(resubmitted = Some(true))
    case TaskResultLost => TaskEndReason(taskResultLost = Some(true))
    case TaskKilled => TaskEndReason(taskKilled = Some(true))
    case UnknownReason => TaskEndReason(unknownReason = Some(true))
    case e: FetchFailed => TaskEndReason(fetchFailed = Some(e))
    case e: ExceptionFailure => TaskEndReason(exceptionFailure = Some(e))
    case e: TaskCommitDenied => TaskEndReason(taskCommitDenied = Some(e))
    case e: ExecutorLostFailure => TaskEndReason(executorLostFailure = Some(e))
  }
}

case class TaskEndEvent(stageId: Int,
                        stageAttemptId: Int,
                        taskType: String,
                        reason: TaskEndReason,
                        taskInfo: TaskInfo,
                        taskMetrics: TaskMetrics)

object TaskEndEvent {
  def apply(s: SparkListenerTaskEnd): TaskEndEvent =
    new TaskEndEvent(
      s.stageId,
      s.stageAttemptId,
      s.taskType,
      TaskEndReason(s.reason),
      TaskInfo(s.taskInfo),
      TaskMetrics(s.taskMetrics)
    )
}

object Props {
  type Props = Map[String, String]
  def apply(properties: Properties): Props =
    properties
        .stringPropertyNames()
        .map(name => name -> properties.getProperty(name))
        .toMap
}

import Props.Props

case class JobStartEvent(jobId: Int,
                         time: Long,
                         stageInfos: Seq[StageInfo],
                         properties: Option[Props] = None)
object JobStartEvent {
  def apply(e: SparkListenerJobStart): JobStartEvent =
    new JobStartEvent(
      e.jobId,
      e.time,
      e.stageInfos.map(StageInfo.apply),
      Option(e.properties).map(Props.apply)
    )
}


case class JobEndEvent(jobId: Int,
                       time: Long,
                       success: Boolean)
object JobEndEvent {
  def apply(e: SparkListenerJobEnd): JobEndEvent =
    JobEndEvent(
      e.jobId,
      e.time,
      e.jobResult match {
        case JobSucceeded => true
        case _ => false
      }
    )
}

case class BlockManagerId(executorId: String, host: String, port: Int)
object BlockManagerId {
  def apply(b: SparkBlockManagerId): BlockManagerId =
    BlockManagerId(
      b.executorId,
      b.host,
      b.port
    )
}

case class BlockManagerAddedEvent(time: Long, blockManagerId: BlockManagerId, maxMem: Long)
object BlockManagerAddedEvent {
  def apply(e: SparkListenerBlockManagerAdded): BlockManagerAddedEvent =
    BlockManagerAddedEvent(
      e.time,
      BlockManagerId(e.blockManagerId),
      e.maxMem
    )
}

case class BlockManagerRemovedEvent(time: Long, blockManagerId: BlockManagerId)
object BlockManagerRemovedEvent {
  def apply(e: SparkListenerBlockManagerRemoved): BlockManagerRemovedEvent =
    BlockManagerRemovedEvent(
      e.time,
      BlockManagerId(e.blockManagerId)
    )
}

case class ExecutorInfo(executorHost: String,
                        totalCores: Int,
                        logUrlMap: Map[String, String])
object ExecutorInfo {
  def apply(e: SparkExecutorInfo): ExecutorInfo =
    ExecutorInfo(
      e.executorHost,
      e.totalCores,
      e.logUrlMap
    )
}

case class ExecutorAddedEvent(time: Long, executorId: String, executorInfo: ExecutorInfo)
object ExecutorAddedEvent {
  def apply(e: SparkListenerExecutorAdded): ExecutorAddedEvent =
    ExecutorAddedEvent(
      e.time,
      e.executorId,
      ExecutorInfo(e.executorInfo)
    )
}

case class ExecutorMetricsUpdateEvent(execId: String,
                                      taskMetrics: Seq[(Long, Int, Int, TaskMetrics)])
object ExecutorMetricsUpdateEvent {
  def apply(e: SparkListenerExecutorMetricsUpdate): ExecutorMetricsUpdateEvent =
    ExecutorMetricsUpdateEvent(
      e.execId,
      e.taskMetrics.map(p => (p._1, p._2, p._3, TaskMetrics(p._4)))
    )
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
