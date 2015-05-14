package org.hammerlab.spear

import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait JobEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Job events
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    val numTasks = jobStart.stageInfos.map(_.numTasks).sum

    val job = Job.newBuilder
              .id(jobStart.jobId)
              .time(makeDuration(jobStart.time))
              .stageIDs(jobStart.stageIds)
              .properties(SparkIDL.properties(jobStart.properties))
              .taskCounts(Counts.newBuilder.num(numTasks).result)
              .stageCounts(Counts.newBuilder.num(jobStart.stageIds.length).result)
              .result()
    db.insert(job)



    jobStart.stageInfos.foreach(si => {
      db.findAndUpsertOne(
        Q(Stage)
        .where(_.id eqs si.stageId)
        .and(_.attempt eqs si.attemptId)
        .findAndModify(_.name setTo si.name)
        .and(_.taskCounts.sub.field(_.num) setTo si.numTasks)
        .and(_.rddIDs setTo si.rddInfos.map(_.id))
        .and(_.details setTo si.details)
        .and(_.time setTo makeDuration(si.submissionTime, si.completionTime))
        .and(_.failureReason setTo si.failureReason)
        .and(_.jobId setTo jobStart.jobId)
      )

      db.findAndUpsertOne(
        Q(StageJobJoin)
          .where(_.stageId eqs si.stageId)
          .findAndModify(_.jobId setTo jobStart.jobId)
      )

    })

    upsertRDDs(jobStart.stageInfos.flatMap(_.rddInfos))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    db.findAndUpdateOne(
      Q(Job)
      .where(_.id eqs jobEnd.jobId)
      .findAndModify(_.time.sub.field(_.end) setTo jobEnd.time)
      .and(_.succeeded setTo (jobEnd.jobResult == JobSucceeded))
    )
  }
}
