
package org.hammerlab.spear

import com.mongodb.casbah.MongoClient
import org.scalatest.{Matchers, FunSuite}

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
import org.apache.spark.storage.{RDDInfo => SparkRDDInfo, StorageLevel => SparkStorageLevel, BlockStatus => SparkBlockStatus, RDDBlockId, BlockId}
import org.apache.spark.scheduler.{StageInfo => SparkStageInfo, TaskInfo => SparkTaskInfo, SparkListenerExecutorMetricsUpdate, SparkListenerTaskEnd, SparkListenerTaskStart, TaskLocality}

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

case class A(n: Int)
case class B(a: A)
case class C(a: (Int, A))
case class D(as: List[(Int, A)])
case class E(a: (Int, Int, A))
case class F(as: List[(Int, Int, A)])

case class G(a: (Long, Int, Int, A))
case class H(as: List[(Long, Int, Int, A)])

case class I(name: String, a: (Long, Int, Int, A))
case class J(name: String, as: List[(Long, Int, Int, A)])

case class K(name: String, as: List[(Long, Int, Int, TaskMetrics)])
case class L(name: String, as: List[(Long, Int, Int, InputMetrics)])

case class M(a: Option[A])
case class N(name: String, as: List[(Long, Int, Int, M)])
case class O(name: String, as: List[M])

case class P(as: List[(Long, Int, Int, M)])
case class Q(as: List[M])
case class R(as: List[(Int, M)])
case class S(as: List[(Int, Int, M)])

case class T(as: List[A])

class SpearTest extends FunSuite with Matchers {

  val client = MongoClient("localhost", 27017)

  val db = client("test")
  val collection = db("test")
  collection.drop()

//  val mongo = new Fongo("fake-mongo")
//  val db = mongo.getDB("test-db")
//  val collection = db.getCollection("test-collection")

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
