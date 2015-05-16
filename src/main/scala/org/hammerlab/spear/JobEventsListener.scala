package org.hammerlab.spear

import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait JobEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Job events
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    val numTasks = jobStart.stageInfos.map(_.numTasks).sum

    val taskCounts = Counts.newBuilder.num(numTasks).started(0).failed(0).running(0).succeeded(0).result
    val stageCounts = Counts.newBuilder.num(jobStart.stageIds.length).started(0).failed(0).running(0).succeeded(0).result

    db.findAndUpsertOne(
      getJob(jobStart.jobId)
        .findAndModify(_.time setTo makeDuration(jobStart.time))
        .and(_.stageIDs setTo jobStart.stageIds)
        .and(_.properties setTo SparkIDL.properties(jobStart.properties))
        .and(_.taskCounts setTo taskCounts)
        .and(_.stageCounts setTo stageCounts)
    )

    jobStart.stageInfos.foreach(si => {
      val taskCounts = Counts.newBuilder.num(si.numTasks).started(0).failed(0).running(0).succeeded(0).result
      db.findAndUpsertOne(
        getStage(si.stageId, si.attemptId)
          .findAndModify(_.name setTo si.name)
          // TODO(ryan): possible race if the stage starts and metrics from
          // the stage are processed via an Executor heartbeat before this
          // event is processed. Verify that this is impossible, or fix it
          // somehow.
          .and(_.taskCounts setTo taskCounts)
          .and(_.rddIDs setTo si.rddInfos.map(_.id))
          .and(_.details setTo si.details)
          .and(_.time setTo makeDuration(si.submissionTime, si.completionTime))
          .and(_.failureReason setTo si.failureReason)
          .and(_.jobId setTo jobStart.jobId)
      )

      db.findAndUpsertOne(
        getStageJobJoin(si.stageId).findAndModify(_.jobId setTo jobStart.jobId)
      )
    })

    upsertRDDs(jobStart.stageInfos.flatMap(_.rddInfos))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    db.findAndUpdateOne(
      getJob(jobEnd.jobId)
        .findAndModify(_.time.sub.field(_.end) setTo jobEnd.time)
        .and(_.succeeded setTo (jobEnd.jobResult == JobSucceeded))
    )
  }
}
