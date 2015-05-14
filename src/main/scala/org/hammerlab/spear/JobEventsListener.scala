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

    val stages = jobStart.stageInfos.map(si => {
      Stage.newBuilder
      .id(si.stageId)
      .attempt(si.attemptId)
      .name(si.name)
      .numTasks(si.numTasks)
      .rddIDs(si.rddInfos.map(_.id))
      .details(si.details)
      .startTime(si.submissionTime)
      .result()
    })

    // NOTE(ryan): assumes that this happens before the StageSubmitted event,
    // otherwise we'd get duplicate Stage records.
    db.insertAll(stages)

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
