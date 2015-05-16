package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListener}
import com.foursquare.rogue.spindle.SpindleRogue._
import org.hammerlab.spear.TaskEndReasonType.SUCCESS

trait TaskEventsListener extends HasDatabaseService with DBHelpers {
  this: SparkListener =>

  // Task events
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val ti = taskStart.taskInfo

    val existingAttempts = db.fetch(
      getTaskAttempts(taskStart.stageId, taskStart.stageAttemptId, ti.index)
    )
    val runningTasksInc =
      if (existingAttempts.exists(t => t.started() && !t.ended()))
        0
      else
        1

    db.findAndUpsertOne(
      getTask(ti.taskId)
        .findAndModify(_.index setTo ti.index)
        .and(_.attempt setTo ti.attempt)
        .and(_.stageId setTo taskStart.stageId)
        .and(_.stageAttemptId setTo taskStart.stageAttemptId)
        .and(_.time setTo makeDuration(ti.launchTime))
        .and(_.execId setTo ti.executorId)
        .and(_.taskLocality setTo TaskLocality.findById(ti.taskLocality.id))
        .and(_.speculative setTo ti.speculative)
        .and(_.started setTo true),
      returnNew = true
    ) match {
      case None =>
        throw new Exception(s"Error upserting on task start: $taskStart")
      case _ =>
    }

    db.findAndUpdateOne(
      getStage(taskStart.stageId, taskStart.stageAttemptId)
        .findAndModify(_.taskCounts.sub.field(_.started) inc runningTasksInc)
        .and(_.taskCounts.sub.field(_.running) inc runningTasksInc)
        .and(_.taskAttemptCounts.sub.field(_.started) inc 1)
        .and(_.taskAttemptCounts.sub.field(_.running) inc 1)
    )

    db.fetchOne(
      getStageJobJoin(taskStart.stageId).select(_.jobId)
    ).flatten.foreach(jobId => {
      db.findAndUpdateOne(
        getJob(jobId)
          .findAndModify(_.taskCounts.sub.field(_.started) inc runningTasksInc)
          .and(_.taskCounts.sub.field(_.running) inc runningTasksInc)
          .and(_.taskAttemptCounts.sub.field(_.started) inc 1)
          .and(_.taskAttemptCounts.sub.field(_.running) inc 1)
      )
    })
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    db.findAndUpdateOne(
      getTask(taskGettingResult.taskInfo.taskId)
        .findAndModify(_.gettingResult setTo true)
    )
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val reason = SparkIDL.taskEndReason(taskEnd.reason)
    val succeeded = reason.tpeOption().exists(_ == SUCCESS)
    val tm = SparkIDL.taskMetrics(taskEnd.taskMetrics)
    val ti = taskEnd.taskInfo
    val tid = ti.taskId

    // NOTE(ryan): important to compute these *before* updating Task records below.
    val metricsUpdates = Seq((tid, taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskMetrics))
    val metricsDeltas = getTaskMetricsDeltasMap(metricsUpdates)

    val existingAttempts = db.fetch(getTaskAttempts(taskEnd.stageId, taskEnd.stageAttemptId, ti.index))
    val existingSuccess =
      existingAttempts.exists(t =>
        t.taskEndReasonOption().flatMap(_.tpeOption()).exists(_ == SUCCESS)
      )

    val existingFailure =
      existingAttempts.exists(t =>
        t.taskEndReasonOption().flatMap(_.tpeOption()).exists(_ != SUCCESS)
      )

    val runningTasksInc =
      if (existingAttempts.exists(t => t.started() && !t.ended()))
        -1
      else
        0

    val taskAttemptWasStarted =
      db.findAndUpsertOne(
        getTask(tid)
          .findAndModify(_.taskType setTo taskEnd.taskType)
          .and(_.taskEndReason setTo reason)
          .and(_.metrics push tm)
          .and(_.time setTo makeDuration(ti.launchTime, ti.finishTime))
          .and(_.ended setTo true),
        returnNew = true
      ) match {
        case Some(task) => task.started()
        case None =>
          throw new Exception(s"Failed upsert on task end: $taskEnd")
      }

    val runningAttemptsInc = if (taskAttemptWasStarted) -1 else 0

    val (successInc, failureInc) =
      (succeeded, existingSuccess, existingFailure) match {
        case (_, true, _) | (false, false, true) => (0, 0)
        case (true, false, true) => (1, -1)
        case (true, false, false) => (1, 0)
        case (false, false, false) => (0, 1)
      }

    db.findAndUpdateOne(
      getStage(taskEnd.stageId, taskEnd.stageAttemptId)
        .findAndModify(_.taskAttemptCounts.sub.field(s => if (succeeded) s.succeeded else s.failed) inc 1)
        .and(_.taskAttemptCounts.sub.field(_.running) inc runningAttemptsInc)
        .and(_.taskCounts.sub.field(_.succeeded) inc successInc)
        .and(_.taskCounts.sub.field(_.failed) inc failureInc)
        .and(_.taskCounts.sub.field(_.running) inc runningTasksInc)
    )

    db.fetchOne(
      getStageJobJoin(taskEnd.stageId).select(_.jobId)
    ).flatten.foreach(jobId => {
      db.findAndUpdateOne(
        getJob(jobId)
          .findAndModify(_.taskAttemptCounts.sub.field(s => if (succeeded) s.succeeded else s.failed) inc 1)
          .and(_.taskAttemptCounts.sub.field(_.running) inc runningAttemptsInc)
          .and(_.taskCounts.sub.field(_.succeeded) inc successInc)
          .and(_.taskCounts.sub.field(_.failed) inc failureInc)
          .and(_.taskCounts.sub.field(_.running) inc runningTasksInc)
      )
    })

    val taskSeenSet = if (taskAttemptWasStarted) Set(tid) else Set[Long]()
    updateStageMetrics(metricsUpdates, metricsDeltas, taskSeenSet)
    updateExecutorMetrics(ti.executorId, metricsDeltas, taskSeenSet)
  }
}
