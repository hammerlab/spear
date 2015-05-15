package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._
import org.hammerlab.spear.TaskEndReasonType.SUCCESS

trait TaskEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Task events
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val ti = taskStart.taskInfo
    db.findAndUpsertOne(
      Q(Task)
        .where(_.id eqs ti.taskId)
        .findAndModify(_.index setTo ti.index)
        .and(_.attempt setTo ti.attempt)
        .and(_.stageId setTo taskStart.stageId)
        .and(_.stageAttemptId setTo taskStart.stageAttemptId)
        .and(_.time setTo makeDuration(ti.launchTime))
        .and(_.execId setTo ti.executorId)
        .and(_.taskLocality setTo TaskLocality.findById(ti.taskLocality.id))
        .and(_.speculative setTo ti.speculative)
    )

    val q = Q(Stage)
            .where(_.id eqs taskStart.stageId)
            .and(_.attempt eqs taskStart.stageAttemptId)
            .findAndModify(_.taskCounts.sub.field(_.started) inc 1)
            .and(_.taskCounts.sub.field(_.running) inc 1)

    db.findAndUpdateOne(q)

    db.fetchOne(
      Q(StageJobJoin)
        .where(_.stageId eqs taskStart.stageId)
        .select(_.jobId)
    ).flatten.foreach(jobId => {
      db.findAndUpdateOne(
        Q(Job)
          .where(_.id eqs jobId)
          .findAndModify(_.taskCounts.sub.field(_.started) inc 1)
          .and(_.taskCounts.sub.field(_.running) inc 1)
      )
    })
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    db.findAndUpdateOne(
      Q(Task)
      .where(_.id eqs taskGettingResult.taskInfo.taskId)
      .findAndModify(_.gettingResult setTo true)
    )
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val reason = SparkIDL.taskEndReason(taskEnd.reason)
    val success = reason.tpeOption().exists(_ == SUCCESS)
    val tm = SparkIDL.taskMetrics(taskEnd.taskMetrics)
    val ti = taskEnd.taskInfo
    val tid = ti.taskId

    // NOTE(ryan): important to compute these *before* updating Task records below.
    val metricsUpdates = Seq((tid, taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskMetrics))
    val metricsDeltas = getTaskMetricsDeltasMap(metricsUpdates)

    db.findAndUpdateOne(
      Q(Task)
      .where(_.id eqs tid)
      .findAndModify(_.taskType setTo taskEnd.taskType)
      .and(_.taskEndReason setTo reason)
      .and(_.metrics push tm)
      .and(_.time setTo makeDuration(ti.launchTime, ti.finishTime))
    )

    db.findAndUpdateOne(
      Q(Stage)
      .where(_.id eqs taskEnd.stageId)
      .and(_.attempt eqs taskEnd.stageAttemptId)
      .findAndModify(_.taskCounts.sub.field(s => if (success) s.succeeded else s.failed) inc 1)
      .and(_.taskCounts.sub.field(_.running) inc -1)
    )

    db.fetchOne(
      Q(StageJobJoin)
        .where(_.stageId eqs taskEnd.stageId)
        .select(_.jobId)
    ).flatten.foreach(jobId => {
      db.findAndUpdateOne(
        Q(Job)
          .where(_.id eqs jobId)
          .findAndModify(_.taskCounts.sub.field(s => if (success) s.succeeded else s.failed) inc 1)
          .and(_.taskCounts.sub.field(_.running) inc -1)
      )
    })

    updateStageMetrics(metricsUpdates, metricsDeltas)
    updateExecutorMetrics(ti.executorId, metricsDeltas)
  }
}
