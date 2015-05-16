
package org.hammerlab.spear

import com.foursquare.rogue.spindle.{SpindleDBCollectionFactory, SpindleDatabaseService}
import com.foursquare.spindle.UntypedMetaRecord
import com.mongodb.casbah.Imports._

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.scheduler.SparkListener

class Spear(sc: SparkContext,
            mongoHost: String = "localhost",
            mongoPort: Int = 27017)
  extends SparkListener
  with JobEventsListener
  with StageEventsListener
  with TaskEventsListener
  with ExecutorEventsListener
  with ApplicationEventsListener
  with MiscEventsListener
{

  val applicationId = sc.applicationId

  val appId = applicationId

  object db extends SpindleDatabaseService(ConcreteDBCollectionFactory)

  println(s"Creating database for appplication: $applicationId")
  object ConcreteDBCollectionFactory extends SpindleDBCollectionFactory {
    import com.mongodb.{DB, MongoClient}
    lazy val db: DB = new MongoClient(mongoHost, mongoPort).getDB(applicationId)
    override def getPrimaryDB(meta: UntypedMetaRecord) = db
    override def indexCache = None
  }

  // Add executors
  db.insertAll(
    SparkEnv.get.blockManager.master.getMemoryStatus.keySet.toList.map(b =>
      Executor.newBuilder.id(b.executorId).host(b.host).port(b.port).result()
    )
  )

  sc.addSparkListener(this)

  def ensureIndexes(): Unit = {

    val casbahMongoClient = MongoClient(mongoHost, mongoPort)
    val casbahDB = casbahMongoClient(mongoDatabase)
    val applications = casbahDB("applications")
    val jobs = casbahDB("jobs")
    val stages = casbahDB("stages")
    val stageJobJoins = casbahDB("stage_job_joins")
    val tasks = casbahDB("tasks")
    val executors = casbahDB("executors")
    val rdds = casbahDB("rdds")

    applications.ensureIndex(MongoDBObject("id" -> 1))

    jobs.ensureIndex(MongoDBObject("appId" -> 1, "id" -> 1))

    stages.ensureIndex(MongoDBObject("appId" -> 1, "id" -> 1, "attempt" -> 1))

    stageJobJoins.ensureIndex(MongoDBObject("appId" -> 1, "stageId" -> 1))

    tasks.ensureIndex(MongoDBObject("appId" -> 1, "id" -> 1))
    tasks.ensureIndex(MongoDBObject("appId" -> 1, "stageId" -> 1, "stageAttemptId" -> 1, "index" -> 1))

    executors.ensureIndex(MongoDBObject("appId" -> 1, "id" -> 1))
    executors.ensureIndex(MongoDBObject("appId" -> 1, "host" -> 1))

    rdds.ensureIndex(MongoDBObject("appId" -> 1, "id" -> 1))

    casbahMongoClient.close()
  }

  ensureIndexes()
}
