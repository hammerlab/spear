
package org.hammerlab.spear

import com.mongodb.casbah.MongoClient
import com.novus.salat._
import com.novus.salat.global._

import org.apache.spark.{
    TaskEndReason => SparkTaskEndReason,
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
    BlockId
}
import org.apache.spark.scheduler.{
    StageInfo => SparkStageInfo,
    TaskInfo => SparkTaskInfo,
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
    new TaskInfo(t.taskId, t.index, t.attempt, t.launchTime, t.executorId, t.host, t.taskLocality.id, t.speculative)
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
    new StageInfo(s.stageId, s.attemptId, s.name, s.numTasks, s.rddInfos.map(RDDInfo.apply), s.details)
}

case class InputMetrics(bytesRead: Long, recordsRead: Long)
object InputMetrics {
  def apply(i: SparkInputMetrics): InputMetrics = new InputMetrics(i.bytesRead, i.recordsRead)
}

case class OutputMetrics(bytesWritten: Long, recordsWritten: Long)
object OutputMetrics {
  def apply(o: SparkOutputMetrics): OutputMetrics = new OutputMetrics(o.bytesWritten, o.recordsWritten)
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

case class ShuffleWriteMetrics(shuffleBytesWritten: Long, shuffleWriteTime: Long, shuffleRecordsWritten: Long)
object ShuffleWriteMetrics {
  def apply(s: SparkShuffleWriteMetrics): ShuffleWriteMetrics =
    new ShuffleWriteMetrics(s.shuffleBytesWritten, s.shuffleWriteTime, s.shuffleRecordsWritten)
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
    new TaskStartEvent(s.stageId, s.stageAttemptId, TaskInfo(s.taskInfo))
}

case class TaskEndReason(success: Boolean = false,
                         resubmitted: Boolean = false,
                         taskResultLost: Boolean = false,
                         taskKilled: Boolean = false,
                         unknownReason: Boolean = false,
                         fetchFailed: Option[FetchFailed] = None,
                         exceptionFailure: Option[ExceptionFailure] = None,
                         taskCommitDenied: Option[TaskCommitDenied] = None,
                         executorLostFailure: Option[ExecutorLostFailure] = None)

object TaskEndReason {
  def apply(r: SparkTaskEndReason): TaskEndReason = r match {
    case Success => TaskEndReason(success = true)
    case Resubmitted => TaskEndReason(resubmitted = true)
    case TaskResultLost => TaskEndReason(taskResultLost = true)
    case TaskKilled => TaskEndReason(taskKilled = true)
    case UnknownReason => TaskEndReason(unknownReason = true)
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

class Spear extends SparkListener {

  val client = MongoClient("localhost", 27017)
  val db = client("spark")
  val stages = db("stages")
  val completedStages = db("completed_stages")

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val dbo = grater[StageInfo].asDBObject(StageInfo(stageCompleted.stageInfo))
    completedStages.insert(dbo)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val dbo = grater[StageInfo].asDBObject(StageInfo(stageSubmitted.stageInfo))
    stages.insert(dbo)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {

  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

  }
}
