package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListener}
import com.foursquare.rogue.spindle.SpindleRogue._
import com.foursquare.rogue.spindle.{SpindleQuery => Q}

trait ApplicationEventsListener extends HasDatabaseService {
  this: SparkListener =>

  def appId: String

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println(s"Adding application ${applicationStart.appId}")
    db.findAndUpsertOne(
      Q(Application)
        .where(_.id eqs applicationStart.appId.getOrElse(appId))
        .findAndModify(_.name setTo applicationStart.appName)
        .and(_.user setTo applicationStart.sparkUser)
        .and(_.time.sub.field(_.start) setTo applicationStart.time)
    )
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println(s"Ending application ${appId}")
    db.findAndUpsertOne(
      Q(Application)
        .where(_.id eqs appId)
        .findAndModify(_.time.sub.field(_.end) setTo applicationEnd.time)
    )
  }

}
