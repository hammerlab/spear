
package org.hammerlab.spear

import org.scalatest.{Matchers, FunSuite}

import com.novus.salat._
import com.novus.salat.global._
import com.mongodb.casbah.Imports._

import com.github.fakemongo.Fongo

import org.apache.spark.{ Success => SparkSuccess }
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
    BlockId
}
import org.apache.spark.scheduler.{
    StageInfo => SparkStageInfo,
    TaskInfo => SparkTaskInfo,
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

class SpearTest extends FunSuite with Matchers {

  val mongo = new Fongo("fake-mongo")
  val db = mongo.getDB("test-db")
  val collection = db.getCollection("test-collection")

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

  val sparkOutputMetrics = new TestOutputMetrics(1234L, 5678L)

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
      Some(List())
    )

  val sparkListenerTaskEnd = SparkListenerTaskEnd(12, 13, "taskType", SparkSuccess, sparkTaskInfo, sparkTaskMetrics)
  val taskEndEvent = TaskEndEvent(sparkListenerTaskEnd)

  def testSerDe[T <: AnyRef](t: T)(implicit m: Manifest[T]): Unit = {
    val dbo = grater[T].asDBObject(t)
    collection.insert(dbo)
    t should equal(grater[T].asObject(dbo))
  }

  test("SerDe StageInfo") {
    testSerDe(stageInfo)
  }

  test("SerDe RDDInfo") {
    testSerDe(rddInfo)
  }

  test("SerDe BrokenRDDInfo fails") {
    val brokenRDDInfo = BrokenRDDInfo(sparkRDDInfo)
    val dbo = grater[BrokenRDDInfo].asDBObject(brokenRDDInfo)
    try {
      collection.insert(dbo)
      fail("Inserting broken RDDInfo should not have worked, but did.")
    } catch {
      case e: RuntimeException if e.getMessage.startsWith("json can't serialize") =>
      case e: RuntimeException =>
        throw new Exception("Found RuntimeException as expected, but message did not match expected:", e)
      case e: Exception =>
        throw new Exception("Exception occurred but was not a RuntimeException as expected", e)
    }
  }

  test("SerDe TaskInfo") {
    testSerDe[TaskInfo](taskInfo)
  }

  test("SerDe TaskStartEvent") {
    testSerDe(taskStartEvent)
  }

  test("SerDe TaskEndEvent") {
    testSerDe(taskEndEvent)
  }

}
