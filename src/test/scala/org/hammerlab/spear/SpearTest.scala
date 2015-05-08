
package org.hammerlab.spear

import com.mongodb.casbah.{Imports, MongoClient}
import com.novus.salat.transformers.CustomTransformer
import org.scalatest.{Matchers, FunSuite}

import com.novus.salat._
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

  implicit val ctx = new Context {
    override val name: String = "TestContext"
  }
  ctx.registerClassLoader(Thread.currentThread().getContextClassLoader())

  ctx.registerCustomTransformer(new CustomTransformer[TaskMetrics, DBObject]() {
    override def deserialize(b: DBObject): TaskMetrics = {
      new TaskMetrics(
        b.get("hostname").asInstanceOf[String],
        b.get("executorDeserializeTime").asInstanceOf[Long],
        b.get("executorRunTime").asInstanceOf[Long],
        b.get("resultSize").asInstanceOf[Long],
        b.get("jvmGCTime").asInstanceOf[Long],
        b.get("resultSerializationTime").asInstanceOf[Long],
        b.get("memoryBytesSpilled").asInstanceOf[Long],
        b.get("diskBytesSpilled").asInstanceOf[Long],
        Option(b.get("inputMetrics")).map(_.asInstanceOf[DBObject]).map(InputMetrics.fromMongo),
        Option(b.get("outputMetrics")).map(_.asInstanceOf[DBObject]).map(OutputMetrics.fromMongo),
        Option(b.get("shuffleReadMetrics")).map(_.asInstanceOf[DBObject]).map(ShuffleReadMetrics.fromMongo),
        Option(b.get("shuffleWriteMetrics")).map(_.asInstanceOf[DBObject]).map(ShuffleWriteMetrics.fromMongo),
        None
      )
    }

    override def serialize(a: TaskMetrics): DBObject = {
      a.toMongo
    }
  })

  ctx.registerCustomTransformer(new CustomTransformer[InputMetrics, DBObject]() {
    override def deserialize(b: DBObject): InputMetrics = {
      InputMetrics.fromMongo(b)
    }

    override def serialize(a: InputMetrics): DBObject = {
      a.toMongo
    }
  })

  val client = MongoClient("localhost", 27017)

  //println(s"Creating database for appplication: $applicationId")
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

  def testSerDe[T <: AnyRef](t: T)(implicit m: Manifest[T]): Unit = {
    val dbo = grater[T].asDBObject(t)
    collection.insert(dbo)
    t should equal(grater[T].asObject(dbo))
  }

  def testToMongo[T <: AnyRef](t: T): Unit = {
    val dbo = Mongo.to(t)
    collection.insert(dbo)
  }

  test("SerDe StageInfo") {
    testSerDe(stageInfo)
  }

  test("SerDe RDDInfo") {
    testSerDe(rddInfo)
  }

  test("toMongo RDDInfo") {
    testToMongo(rddInfo)
    testToMongo(StorageLevel(4))
    testToMongo(B(A(4)))
    testToMongo(executorMetricsUpdateEvent)
    testToMongo(T(List(A(1),A(2),A(3))))
    testToMongo(TaskEndReason(SparkSuccess))
  }

  test("SerDe BrokenRDDInfo fails") {
    val brokenRDDInfo = BrokenRDDInfo(sparkRDDInfo)
    val dbo = grater[BrokenRDDInfo].asDBObject(brokenRDDInfo)
    try {
      collection.insert(dbo)
      fail("Inserting broken RDDInfo should not have worked, but did.")
    } catch {
      case e: RuntimeException if e.getMessage.startsWith("can't serialize") =>
      case e: RuntimeException =>
        throw new Exception(s"Found RuntimeException as expected, but message did not match expected: ${e.getMessage}", e)
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

  test("SerDe ExecutorMetricsUpdate") {
    testSerDe(executorMetricsUpdateEvent)
  }

  test("SerDes") {
    testSerDe(B(A(4)))
    testSerDe(C((3, A(4))))

    //testSerDe(D(List((1, A(2)), (3, A(4)))))

    testSerDe(E((1, 2, A(3))))
    testSerDe(F(List((1, 2, A(3)), (4, 5, A(6)))))

    testSerDe(G((1L, 2, 3, A(4))))
    testSerDe(H(List((1L, 2, 3, A(4)), (5L, 6, 7, A(8)))))

    testSerDe(I("name", (1L, 2, 3, A(4))))
    testSerDe(J("name", List((1L, 2, 3, A(4)), (5L, 6, 7, A(8)))))

    //testSerDe(K("name", List((1L, 2, 3, taskMetrics), (5L, 6, 7, taskMetrics))))
    testSerDe(L("name", List((1L, 2, 3, inputMetrics), (5L, 6, 7, inputMetrics))))

    //testSerDe(N("name", List((1L, 2, 3, M(Some(A(4)))), (5L, 6, 7, M(Some(A(8)))))))
    testSerDe(O("name", List(M(Some(A(4))), M(Some(A(8))))))

//    testSerDe(P(List((1L, 2, 3, M(Some(A(4)))), (5L, 6, 7, M(Some(A(8)))))))
    testSerDe(Q(List(M(Some(A(4))), M(Some(A(8))))))
//    testSerDe(R(List((1, M(Some(A(4)))), (5, M(Some(A(8)))))))
//    testSerDe(S(List((1, 2, M(Some(A(4)))), (5, 6, M(Some(A(8)))))))
  }

}
