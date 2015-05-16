package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListener}
import com.foursquare.rogue.spindle.SpindleRogue._

trait StageEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Stage events
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    val si = stageSubmitted.stageInfo

    val jobIdOpt = db.fetchOne(getStageJobJoin(si.stageId)).flatMap(_.jobIdOption)

    val existingAttempts = db.fetch(getStageAttempts(si.stageId))
    val runningStagesInc =
      if (existingAttempts.exists(s => s.started() && !s.ended()))
        0
      else
        1

    db.findAndUpsertOne(
      getStage(si.stageId, si.attemptId)
        .findAndModify(_.name setTo si.name)
        .and(_.taskCounts.sub.field(_.num) setTo si.numTasks)
        .and(_.rddIDs setTo si.rddInfos.map(_.id))
        .and(_.details setTo si.details)
        .and(_.time setTo makeDuration(si.submissionTime))
        .and(_.failureReason setTo si.failureReason)
        .and(_.properties setTo SparkIDL.properties(stageSubmitted.properties))
        .and(_.jobId setTo jobIdOpt)
        .and(_.started setTo true),
      returnNew = true
    ) match {
      case None =>
        throw new Exception(s"Error upserting new stage: $stageSubmitted")
      case _ =>
    }

    jobIdOpt.foreach(jobId =>
      db.findAndUpdateOne(
        getJob(jobId)
          .findAndModify(_.stageCounts.sub.field(_.started) inc runningStagesInc)
          .and(_.stageCounts.sub.field(_.running) inc runningStagesInc)
          .and(_.stageAttemptCounts.sub.field(_.started) inc 1)
          .and(_.stageAttemptCounts.sub.field(_.running) inc 1)
      )
    )
    upsertRDDs(si)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val si = stageCompleted.stageInfo

    val succeeded = si.failureReason.isEmpty

    val existingAttempts = db.fetch(getStageAttempts(si.stageId))
    //val existingAttempt = existingAttempts.find(_.attemptOption().exists(_ == si.attemptId))
    val runningStagesInc =
      if (existingAttempts.exists(s => s.started() && !s.ended()))
        -1
      else
        0

    val existingSuccess = existingAttempts.exists(_.succeeded)
    val existingFailure = existingAttempts.exists(s => s.failureReasonOption().isDefined || (s.ended && !s.succeeded))

    val stageAttemptWasStarted =
      db.findAndUpsertOne(
        getStage(si.stageId, si.attemptId)
          // submissionTime sometimes doesn't make it into the StageSubmitted
          // event, likely due to a race on the Spark side.
          .findAndModify(_.time setTo makeDuration(si.submissionTime, si.completionTime))
          .and(_.failureReason setTo si.failureReason)
          .and(_.ended setTo true)
          .and(_.succeeded setTo succeeded),
        returnNew = true
      ) match {
        case Some(stage) => stage.started()
        case None =>
          throw new Exception(s"Failed upsert on stage complete: $stageCompleted")
      }

    val (successInc, failureInc) =
      (succeeded, existingSuccess, existingFailure) match {
        case (_, true, _) | (false, false, true) => (0, 0)
        case (true, false, true) => (1, -1)
        case (true, false, false) => (1, 0)
        case (false, false, false) => (0, 1)
      }

    // NOTE(ryan): could save a query by pulling this off of the Stage record fetched above...
    val jobIdOpt = db.fetchOne(getStageJobJoin(si.stageId)).flatMap(_.jobIdOption)
    jobIdOpt.foreach(jobId =>
      db.findAndUpdateOne(
        getJob(jobId)
          .findAndModify(_.stageAttemptCounts.sub.field(s => if (succeeded) s.succeeded else s.failed) inc 1)
          .and(_.stageAttemptCounts.sub.field(_.running) inc (if (stageAttemptWasStarted) -1 else 0))
          .and(_.stageCounts.sub.field(_.succeeded) inc successInc)
          .and(_.stageCounts.sub.field(_.failed) inc failureInc)
          .and(_.stageCounts.sub.field(_.running) inc runningStagesInc)
      )
    )
  }

}
