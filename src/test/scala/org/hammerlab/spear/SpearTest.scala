
package org.hammerlab.spear

import com.mongodb.casbah.{MongoCollection, MongoClient}
import org.scalatest.{Matchers, FunSuite}

import com.github.fakemongo.Fongo

import org.apache.spark.{
    Success => SparkSuccess
}
import org.apache.spark.executor.{
    TaskMetrics => SparkTaskMetrics,
    ShuffleWriteMetrics => SparkShuffleWriteMetrics,
    ShuffleReadMetrics => SparkShuffleReadMetrics,
    InputMetrics => SparkInputMetrics,
    OutputMetrics => SparkOutputMetrics,
    DataWriteMethod,
    DataReadMethod
}
import org.apache.spark.storage.{
    RDDInfo => SparkRDDInfo,
    StorageLevel => SparkStorageLevel,
    BlockStatus => SparkBlockStatus,
    RDDBlockId,
    BlockId
}
import org.apache.spark.scheduler.{
    StageInfo => SparkStageInfo,
    TaskInfo => SparkTaskInfo,
    SparkListenerExecutorMetricsUpdate,
    SparkListenerTaskEnd,
    SparkListenerTaskStart,
    TaskLocality
}

case class BrokenRDDInfo(id: Int,
                         name: String,
                         numPartitions: Int,
                         storageLevel: SparkStorageLevel)

object BrokenRDDInfo {
  def apply(r: SparkRDDInfo): BrokenRDDInfo =
    new BrokenRDDInfo(
      r.id,
      r.name,
      r.numPartitions,
      r.storageLevel
    )
}

case class TestTaskMetrics(override val hostname: String,
                           override val executorDeserializeTime: Long,
                           override val executorRunTime: Long,
                           override val resultSize: Long,
                           override val jvmGCTime: Long,
                           override val resultSerializationTime: Long,
                           override val memoryBytesSpilled: Long,
                           override val diskBytesSpilled: Long,
                           override val inputMetrics: Option[SparkInputMetrics],
                           outputMetricsOpt: Option[SparkOutputMetrics],
                           override val shuffleReadMetrics: Option[SparkShuffleReadMetrics],
                           shuffleWriteMetricsOpt: Option[SparkShuffleWriteMetrics],
                           updatedBlocksOpt: Option[Seq[(BlockId, SparkBlockStatus)]]) extends SparkTaskMetrics {
  outputMetrics = outputMetricsOpt
  shuffleWriteMetrics = shuffleWriteMetricsOpt
  updatedBlocks = updatedBlocksOpt
}

class TestOutputMetrics(override val bytesWritten: Long,
                        override val recordsWritten: Long) extends SparkOutputMetrics(DataWriteMethod.Hadoop)

case class TestShuffleReadMetrics(override val remoteBlocksFetched: Int,
                                  override val localBlocksFetched: Int,
                                  override val fetchWaitTime: Long,
                                  override val remoteBytesRead: Long,
                                  override val localBytesRead: Long,
                                  override val recordsRead: Long) extends SparkShuffleReadMetrics

case class TestShuffleWriteMetrics(override val shuffleBytesWritten: Long,
                                   override val shuffleWriteTime: Long,
                                   override val shuffleRecordsWritten: Long) extends SparkShuffleWriteMetrics


class SpearTest extends FunSuite with Matchers with MongoTestCollection {

  val sparkRDDInfo = new SparkRDDInfo(1, "a", 2, SparkStorageLevel.MEMORY_AND_DISK)
  val rddInfo = RDDInfo(sparkRDDInfo)

  val sparkStageInfo = new SparkStageInfo(3, 4, "stage", 5, List(sparkRDDInfo), "details")
  val stageInfo = StageInfo(sparkStageInfo)

  val sparkTaskInfo = new SparkTaskInfo(6L, 7, 8, 9L, "executorId", "host", TaskLocality.ANY, speculative = true)
  val taskInfo = TaskInfo(sparkTaskInfo)

  val sparkListenerTaskStart = SparkListenerTaskStart(10, 11, sparkTaskInfo)
  val taskStartEvent = TaskStartEvent(sparkListenerTaskStart)

  val sparkInputMetrics = new SparkInputMetrics(DataReadMethod.Memory)
  sparkInputMetrics.incBytesRead(123L)
  sparkInputMetrics.incRecordsRead(456L)

  val inputMetrics = InputMetrics(sparkInputMetrics)

  val sparkOutputMetrics = new TestOutputMetrics(1234L, 5678L)
  val outputMetrics = OutputMetrics(sparkOutputMetrics)

  val sparkShuffleReadMetrics = TestShuffleReadMetrics(12, 13, 14L, 15L, 16L, 17L)
  val sparkShuffleWriteMetrics = TestShuffleWriteMetrics(18L, 19L, 20L)

  val sparkTaskMetrics =
    TestTaskMetrics(
      "hostname",
      12, 13, 14, 15, 16, 17, 18,
      Some(sparkInputMetrics),
      Some(sparkOutputMetrics),
      Some(sparkShuffleReadMetrics),
      Some(sparkShuffleWriteMetrics),
      Some(
        Seq(
          (
            RDDBlockId(1,2),
              SparkBlockStatus(
                SparkStorageLevel.MEMORY_ONLY,
                2L,
                3L,
                4L
              )
            )
        )
      )
    )
  val taskMetrics = TaskMetrics(sparkTaskMetrics)

  val sparkListenerTaskEnd = SparkListenerTaskEnd(12, 13, "taskType", SparkSuccess, sparkTaskInfo, sparkTaskMetrics)
  val taskEndEvent = TaskEndEvent(sparkListenerTaskEnd)

  val sparkExecutorMetricsUpdate = SparkListenerExecutorMetricsUpdate("execId", List((1L, 2, 3, sparkTaskMetrics)))
  val executorMetricsUpdateEvent = ExecutorMetricsUpdateEvent(sparkExecutorMetricsUpdate)

  def testToMongo[T <: AnyRef](t: T): Unit = {
    val dbo = MongoCaseClassSerializer.to(t)
    collection.insert(dbo)
  }

  test("toMongo RDDInfo") {
    testToMongo(rddInfo)
    testToMongo(StorageLevel(4))
    testToMongo(B(A(4)))
    testToMongo(executorMetricsUpdateEvent)
    testToMongo(T(List(A(1),A(2),A(3))))
    testToMongo(TaskEndReason(SparkSuccess))
  }

}
