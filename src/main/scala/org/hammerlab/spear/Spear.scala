
package org.hammerlab.spear

import com.foursquare.rogue.spindle.{SpindleDBCollectionFactory, SpindleDatabaseService}
import com.foursquare.spindle.UntypedMetaRecord
import com.mongodb.{DB, MongoClient}

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
  with MiscEventsListener
{

  val applicationId = sc.applicationId

  object db extends SpindleDatabaseService(ConcreteDBCollectionFactory)

  println(s"Creating database for appplication: $applicationId")
  object ConcreteDBCollectionFactory extends SpindleDBCollectionFactory {
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

}
