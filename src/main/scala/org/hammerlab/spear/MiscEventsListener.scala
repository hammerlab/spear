package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerUnpersistRDD, SparkListenerBlockManagerRemoved, SparkListenerBlockManagerAdded, SparkListenerEnvironmentUpdate}

trait MiscEventsListener {
  this: SparkListener =>

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    // TODO(ryan)
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    // TODO(ryan)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    // TODO(ryan)
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    // TODO(ryan)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    // TODO(ryan)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    // TODO(ryan)
  }
}
