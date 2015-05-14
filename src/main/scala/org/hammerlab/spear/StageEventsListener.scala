package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait StageEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Stage events
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    val si = stageSubmitted.stageInfo

    val jobIdOpt = db.fetchOne(Q(StageJobJoin).where(_.stageId eqs si.stageId)).flatMap(_.jobIdOption)

    db.findAndUpsertOne(
      Q(Stage)
      .where(_.id eqs si.stageId)
      .and(_.attempt eqs si.attemptId)
      .findAndModify(_.name setTo si.name)
      .and(_.taskCounts.sub.field(_.num) setTo si.numTasks)
      .and(_.rddIDs setTo si.rddInfos.map(_.id))
      .and(_.details setTo si.details)
      .and(_.time setTo makeDuration(si.submissionTime))
      .and(_.failureReason setTo si.failureReason)
      .and(_.properties setTo SparkIDL.properties(stageSubmitted.properties))
      .and(_.jobId setTo jobIdOpt)
    )

    upsertRDDs(si)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val si = stageCompleted.stageInfo
    db.findAndUpdateOne(
      Q(Stage)
      .where(_.id eqs si.stageId)
      .and(_.attempt eqs si.attemptId)
      // submissionTime sometimes doesn't make it into the StageSubmitted
      // event, likely due to a race on the Spark side.
      .findAndModify(_.time setTo makeDuration(si.submissionTime, si.completionTime))
      .and(_.failureReason setTo si.failureReason)
    )
  }

}
