package org.hammerlab.spear

import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait JobEventsListener extends HasDatabaseService {
  this: SparkListener =>

  // Job events
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    val job = Job.newBuilder
              .id(jobStart.jobId)
              .startTime(jobStart.time)
              .stageIDs(jobStart.stageIds)
              .properties(SparkIDL.properties(jobStart.properties))
              .result()
    db.insert(job)

    jobStart.stageInfos.foreach(si => {
      db.findAndUpsertOne(
        Q(Stage)
        .where(_.id eqs si.stageId)
        .and(_.attempt eqs si.attemptId)
        .findAndModify(_.name setTo si.name)
        .and(_.numTasks setTo si.numTasks)
        .and(_.rddIDs setTo si.rddInfos.map(_.id))
        .and(_.details setTo si.details)
        .and(_.startTime setTo si.submissionTime)
        .and(_.endTime setTo si.completionTime)
        .and(_.failureReason setTo si.failureReason)
      )


    })

    val rdds = jobStart.stageInfos.flatMap(_.rddInfos).map(ri => {
      RDD.newBuilder
      .id(ri.id)
      .name(ri.name)
      .numPartitions(ri.numPartitions)
      .storageLevel(SparkIDL.storageLevel(ri.storageLevel))
      .result()
    })

    db.insertAll(rdds)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    db.findAndUpdateOne(
      Q(Job)
      .where(_.id eqs jobEnd.jobId)
      .findAndModify(_.endTime setTo jobEnd.time)
      .and(_.succeeded setTo (jobEnd.jobResult == JobSucceeded))
    )
  }
}
