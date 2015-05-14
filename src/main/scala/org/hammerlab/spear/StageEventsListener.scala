package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._

trait StageEventsListener extends HasDatabaseService {
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
      .and(_.numTasks setTo si.numTasks)
      .and(_.rddIDs setTo si.rddInfos.map(_.id))
      .and(_.details setTo si.details)
      .and(_.startTime setTo si.submissionTime)
      .and(_.endTime setTo si.completionTime)
      .and(_.failureReason setTo si.failureReason)
      .and(_.properties setTo SparkIDL.properties(stageSubmitted.properties))
      .and(_.jobId setTo jobIdOpt)
    )

    // TODO(ryan): verify whether this will lead to duplicate RDD records, e.g.
    // if stages share an RDD; switch to upserting RDD records one by one
    // instead if so.
    db.insertAll(
      si.rddInfos.map(ri => {
        RDD.newBuilder
        .id(ri.id)
        .name(ri.name)
        .numPartitions(ri.numPartitions)
        .storageLevel(SparkIDL.storageLevel(ri.storageLevel))
        .numCachedPartitions(ri.numCachedPartitions)
        .memSize(ri.memSize)
        .diskSize(ri.diskSize)
        .tachyonSize(ri.tachyonSize)
        .result()
      })
    )
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val si = stageCompleted.stageInfo
    db.findAndUpdateOne(
      Q(Stage)
      .where(_.id eqs si.stageId)
      .and(_.attempt eqs si.attemptId)
      .findAndModify(_.endTime setTo si.completionTime)
      // submissionTime sometimes doesn't make it into the StageSubmitted
      // event, likely due to a race on the Spark side.
      .and(_.startTime setTo si.submissionTime)
      .and(_.failureReason setTo si.failureReason)
    )
  }

}
