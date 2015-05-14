package org.hammerlab.spear

import java.util.Properties
import scala.collection.JavaConversions._

import org.hammerlab.spear.TaskEndReasonType._

import org.apache.spark.{
    TaskEndReason => SparkTaskEndReason,
    Success => SparkSuccess,
    UnknownReason => SparkUnknownReason,
    TaskKilled => SparkTaskKilled,
    TaskResultLost => SparkTaskResultLost,
    Resubmitted => SparkResubmitted,
    ExecutorLostFailure => SparkExecutorLostFailure,
    TaskCommitDenied => SparkTaskCommitDenied,
    ExceptionFailure => SparkExceptionFailure,
    FetchFailed => SparkFetchFailed
}
import org.apache.spark.executor.{
    TaskMetrics => SparkTaskMetrics,
    ShuffleWriteMetrics => SparkShuffleWriteMetrics,
    ShuffleReadMetrics => SparkShuffleReadMetrics,
    InputMetrics => SparkInputMetrics,
    OutputMetrics => SparkOutputMetrics
}
import org.apache.spark.storage.{
    StorageLevel => SparkStorageLevel,
    BlockStatus => SparkBlockStatus,
    BlockManagerId => SparkBlockManagerId,
    BlockId
}

object SparkIDL {

  def apply(im: SparkInputMetrics) = inputMetrics(im)
  def inputMetrics(im: SparkInputMetrics): InputMetrics = {
    InputMetrics.newBuilder
      .readMethod(DataReadMethod.findById(im.readMethod.id))
      .bytesRead(im.bytesRead)
      .recordsRead(im.recordsRead)
      .result()
  }

  def apply(om: SparkOutputMetrics) = outputMetrics(om)
  def outputMetrics(om: SparkOutputMetrics): OutputMetrics = {
    OutputMetrics.newBuilder
      .writeMethod(DataWriteMethod.findById(om.writeMethod.id))
      .bytesWritten(om.bytesWritten)
      .recordsWritten(om.recordsWritten)
      .result()
  }

  def apply(sm: SparkShuffleReadMetrics) = shuffleReadMetrics(sm)
  def shuffleReadMetrics(sm: SparkShuffleReadMetrics): ShuffleReadMetrics = {
    ShuffleReadMetrics.newBuilder
      .remoteBlocksFetched(sm.remoteBlocksFetched)
      .localBlocksFetched(sm.localBlocksFetched)
      .fetchWaitTime(sm.fetchWaitTime)
      .remoteBytesRead(sm.remoteBytesRead)
      .localBytesRead(sm.localBytesRead)
      .recordsRead(sm.recordsRead)
      .result()
  }

  def apply(sm: SparkShuffleWriteMetrics) = shuffleWriteMetrics(sm)
  def shuffleWriteMetrics(sm: SparkShuffleWriteMetrics): ShuffleWriteMetrics = {
    ShuffleWriteMetrics.newBuilder
      .shuffleBytesWritten(sm.shuffleBytesWritten)
      .shuffleWriteTime(sm.shuffleWriteTime)
      .shuffleRecordsWritten(sm.shuffleRecordsWritten)
      .result()
  }

  def updatedBlocks(ub: Seq[(BlockId, SparkBlockStatus)]): Seq[UpdatedBlock] = {
    ub.map(updatedBlock)
  }

  def updatedBlock(ub: (BlockId, SparkBlockStatus)): UpdatedBlock = {
    UpdatedBlock.newBuilder
      .blockId(ub._1.name)
      .blockStatus(blockStatus(ub._2))
      .result()
  }

  def apply(sl: SparkStorageLevel) = storageLevel(sl)
  def storageLevel(sl: SparkStorageLevel): StorageLevel = {
    StorageLevel.newBuilder
      .useDisk(sl.useDisk)
      .useMemory(sl.useMemory)
      .useOffHeap(sl.useOffHeap)
      .deserialized(sl.deserialized)
      .replication(sl.replication)
      .result()
  }

  def blockStatus(bs: SparkBlockStatus): BlockStatus = {
    BlockStatus.newBuilder
      .storageLevel(storageLevel(bs.storageLevel))
      .memSize(bs.memSize)
      .diskSize(bs.diskSize)
      .tachyonSize(bs.tachyonSize)
      .result()
  }

  def apply(tm: SparkTaskMetrics) = taskMetrics(tm)
  def taskMetrics(tm: SparkTaskMetrics): TaskMetrics = {
    TaskMetrics.newBuilder
      .hostname(tm.hostname)
      .executorDeserializeTime(tm.executorDeserializeTime)
      .executorRunTime(tm.executorRunTime)
      .resultSize(tm.resultSize)
      .jvmGCTime(tm.jvmGCTime)
      .resultSerializationTime(tm.resultSerializationTime)
      .memoryBytesSpilled(tm.memoryBytesSpilled)
      .diskBytesSpilled(tm.diskBytesSpilled)
      .inputMetrics(tm.inputMetrics.map(inputMetrics))
      .outputMetrics(tm.outputMetrics.map(outputMetrics))
      .shuffleReadMetrics(tm.shuffleReadMetrics.map(shuffleReadMetrics))
      .shuffleWriteMetrics(tm.shuffleWriteMetrics.map(shuffleWriteMetrics))
      .updatedBlocks(tm.updatedBlocks.map(updatedBlocks))
      .result()
  }

  def combineMetrics(a: InputMetrics, b: Option[InputMetrics], add: Boolean): InputMetrics = {
    InputMetrics.newBuilder
      .readMethod(a.readMethodOption)
      .bytesRead(a.bytesRead + (if (add) 1 else -1)*b.map(_.bytesRead).getOrElse(0L))
      .recordsRead(a.recordsRead + (if (add) 1 else -1)*b.map(_.recordsRead).getOrElse(0L))
      .result()
  }

  def combineMetrics(a: OutputMetrics, b: Option[OutputMetrics], add: Boolean): OutputMetrics = {
    OutputMetrics.newBuilder
      .writeMethod(a.writeMethodOption)
      .bytesWritten(a.bytesWritten + (if (add) 1 else -1)*b.map(_.bytesWritten).getOrElse(0L))
      .recordsWritten(a.recordsWritten + (if (add) 1 else -1)*b.map(_.recordsWritten).getOrElse(0L))
      .result()
  }

  def combineMetrics(a: ShuffleReadMetrics, b: Option[ShuffleReadMetrics], add: Boolean): ShuffleReadMetrics = {
    ShuffleReadMetrics.newBuilder
      .remoteBlocksFetched(a.remoteBlocksFetched + (if (add) 1 else -1)*b.map(_.remoteBlocksFetched).getOrElse(0))
      .localBlocksFetched(a.localBlocksFetched + (if (add) 1 else -1)*b.map(_.localBlocksFetched).getOrElse(0))
      .fetchWaitTime(a.fetchWaitTime + (if (add) 1 else -1)*b.map(_.fetchWaitTime).getOrElse(0L))
      .remoteBytesRead(a.remoteBytesRead + (if (add) 1 else -1)*b.map(_.remoteBytesRead).getOrElse(0L))
      .localBytesRead(a.localBytesRead + (if (add) 1 else -1)*b.map(_.localBytesRead).getOrElse(0L))
      .recordsRead(a.recordsRead + (if (add) 1 else -1)*b.map(_.recordsRead).getOrElse(0L))
      .result()
  }

  def combineMetrics(a: ShuffleWriteMetrics, b: Option[ShuffleWriteMetrics], add: Boolean): ShuffleWriteMetrics = {
    ShuffleWriteMetrics.newBuilder
      .shuffleBytesWritten(a.shuffleBytesWritten + (if (add) 1 else -1)*b.map(_.shuffleBytesWritten).getOrElse(0L))
      .shuffleWriteTime(a.shuffleWriteTime + (if (add) 1 else -1)*b.map(_.shuffleWriteTime).getOrElse(0L))
      .shuffleRecordsWritten(a.shuffleRecordsWritten + (if (add) 1 else -1)*b.map(_.shuffleRecordsWritten).getOrElse(0L))
      .result()
  }

  def combineMetrics(a: TaskMetrics, b: Option[TaskMetrics], add: Boolean): TaskMetrics = {
    TaskMetrics.newBuilder
      .hostname(a.hostnameOption)
      .executorDeserializeTime(a.executorDeserializeTime + (if (add) 1 else -1)*b.map(_.executorDeserializeTime).getOrElse(0L))
      .executorRunTime(a.executorRunTime + (if (add) 1 else -1)*b.map(_.executorRunTime).getOrElse(0L))
      .resultSize(a.resultSize + (if (add) 1 else -1)*b.map(_.resultSize).getOrElse(0L))
      .jvmGCTime(a.jvmGCTime + (if (add) 1 else -1)*b.map(_.jvmGCTime).getOrElse(0L))
      .resultSerializationTime(a.resultSerializationTime + (if (add) 1 else -1)*b.map(_.resultSerializationTime).getOrElse(0L))
      .memoryBytesSpilled(a.memoryBytesSpilled + (if (add) 1 else -1)*b.map(_.memoryBytesSpilled).getOrElse(0L))
      .diskBytesSpilled(a.diskBytesSpilled + (if (add) 1 else -1)*b.map(_.diskBytesSpilled).getOrElse(0L))
      .inputMetrics(a.inputMetricsOption.map(combineMetrics(_, b.flatMap(_.inputMetricsOption), add = add)))
      .outputMetrics(a.outputMetricsOption.map(combineMetrics(_, b.flatMap(_.outputMetricsOption), add = add)))
      .shuffleReadMetrics(a.shuffleReadMetricsOption.map(combineMetrics(_, b.flatMap(_.shuffleReadMetricsOption), add = add)))
      .shuffleWriteMetrics(a.shuffleWriteMetricsOption.map(combineMetrics(_, b.flatMap(_.shuffleWriteMetricsOption), add = add)))
      .result()
  }

  def blockManagerId(id: SparkBlockManagerId): BlockManagerId = {
    BlockManagerId.newBuilder
      .executorID(id.executorId)
      .host(id.host)
      .port(id.port)
      .result()
  }

  def fetchFailed(ff: SparkFetchFailed): FetchFailed = {
    FetchFailed.newBuilder
      .bmAddress(blockManagerId(ff.bmAddress))
      .shuffleId(ff.shuffleId)
      .mapId(ff.mapId)
      .reduceId(ff.reduceId)
      .message(ff.message)
      .result()
  }

  def stackTraceElement(se: StackTraceElement): StackTraceElem = {
    StackTraceElem.newBuilder
      .declaringClass(se.getClassName)
      .methodName(se.getMethodName)
      .fileName(se.getFileName)
      .lineNumber(se.getLineNumber)
      .result()
  }

  def stackTrace(st: Seq[StackTraceElement]): Seq[StackTraceElem] = {
    st.map(stackTraceElement)
  }

  def exceptionFailure(ef: SparkExceptionFailure): ExceptionFailure = {
    ExceptionFailure.newBuilder
      .className(ef.className)
      .description(ef.description)
      .stackTrace(stackTrace(ef.stackTrace))
      .fullStackTrace(ef.fullStackTrace)
      .metrics(ef.metrics.map(taskMetrics))
      .result()
  }

  def taskCommitDenied(td: SparkTaskCommitDenied): TaskCommitDenied = {
    TaskCommitDenied.newBuilder
      .jobID(td.jobID)
      .partitionID(td.partitionID)
      .attemptID(td.attemptID)
      .result()
  }

  def executorLostFailure(lf: SparkExecutorLostFailure): ExecutorLostFailure = {
    ExecutorLostFailure.newBuilder
      .execId(lf.execId)
      .result()
  }

  def taskEndReason(r: SparkTaskEndReason): TaskEndReason = {
    val builder = TaskEndReason.newBuilder

    r match {
      case SparkSuccess =>
        builder.tpe(SUCCESS)
      case SparkResubmitted =>
        builder.tpe(RESUBMITTED)
      case SparkTaskResultLost =>
        builder.tpe(TASK_RESULT_LOST)
      case SparkTaskKilled =>
        builder.tpe(TASK_KILLED)
      case SparkUnknownReason =>
        builder.tpe(UNKNOWN_REASON)
      case e: SparkFetchFailed =>
        builder.tpe(FETCH_FAILED).fetchFailed(fetchFailed(e))
      case e: SparkExceptionFailure =>
        builder.tpe(EXCEPTION_FAILURE).exceptionFailure(exceptionFailure(e))
      case e: SparkTaskCommitDenied =>
        builder.tpe(TASK_COMMIT_DENIED).taskCommitDenied(taskCommitDenied(e))
      case e: SparkExecutorLostFailure =>
        builder.tpe(EXECUTOR_LOST_FAILURE).executorLostFailure(executorLostFailure(e))
    }

    builder.result()
  }

  def properties(props: Properties): Option[Map[String, String]] =
    Option(props).map(p => p.keySet().map(_.toString).map(k => k -> p.getProperty(k)).toMap)
}

