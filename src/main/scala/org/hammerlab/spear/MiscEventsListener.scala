package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListener, SparkListenerUnpersistRDD, SparkListenerBlockManagerRemoved, SparkListenerBlockManagerAdded, SparkListenerEnvironmentUpdate}
import org.hammerlab.spear.SparkTypedefs.ApplicationID
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait MiscEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  def appId: ApplicationID

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    db.findAndUpsertOne(
      Q(Environment)
        .where(_.appId eqs appId)
        .findAndModify(
          _.env setTo
            environmentUpdate.environmentDetails.mapValues(
              _.map(
                p => Seq(p._1, p._2)
              )
            ).toMap
        )
    )
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    db.findAndUpsertOne(
      getExecutor(blockManagerAdded.blockManagerId.executorId)
        .findAndModify(_.host setTo blockManagerAdded.blockManagerId.host)
        .and(_.port setTo blockManagerAdded.blockManagerId.port)
        .and(_.maxMem setTo blockManagerAdded.maxMem)
        .and(_.time.sub.field(_.start) setTo blockManagerAdded.time)
    )
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    db.findAndUpsertOne(
      getExecutor(blockManagerRemoved.blockManagerId.executorId)
        .findAndModify(_.host setTo blockManagerRemoved.blockManagerId.host)
        .and(_.port setTo blockManagerRemoved.blockManagerId.port)
        .and(_.time.sub.field(_.start) setTo blockManagerRemoved.time)
    )
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    db.findAndUpsertOne(
      getRDD(unpersistRDD.rddId).findAndModify(_.unpersisted setTo true)
    )
  }
}
