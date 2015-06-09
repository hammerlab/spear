package org.hammerlab.spear

import org.apache.spark.scheduler.{SparkListenerExecutorRemoved, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListener}
import com.foursquare.rogue.spindle.{SpindleQuery => Q}
import com.foursquare.rogue.spindle.SpindleRogue._
import org.hammerlab.spear.SparkTypedefs.TaskID

trait ExecutorEventsListener
  extends HasDatabaseService
  with DBHelpers
{
  this: SparkListener =>

  // Executor events
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    val metrics = executorMetricsUpdate.taskMetrics
    if (metrics.size > 0) {

      val eid = executorMetricsUpdate.execId

      // NOTE(ryan): important to do this *before* updating Task records below :-\
      val metricsDeltas: Map[TaskID, TaskMetrics] = getTaskMetricsDeltasMap(metrics)

      // Update Task records
      val tasksAlreadyExisted =
        metrics.flatMap {
          case (taskId, stageId, stageAttempt, taskMetrics) =>
            db.findAndUpsertOne(
              getTask(taskId)
              .findAndModify(_.metrics push SparkIDL.taskMetrics(taskMetrics))
              .and(_.stageId setTo stageId)
              .and(_.stageAttemptId setTo stageAttempt)
            ) match {
              case None =>
                System.err.println(
                  s"Got executor $eid metrics update about previously-unheard-of task: $taskId in stage $stageId.$stageAttempt."
                )
                None
              case _ =>
                Some(taskId)
            }
        }.toSet

      // Update Executor metrics
      updateExecutorMetrics(eid, metricsDeltas, tasksAlreadyExisted)

      // Update Stage metrics
      updateStageMetrics(metrics, metricsDeltas, tasksAlreadyExisted)
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

    val (host, portOpt) = executorAdded.executorInfo.executorHost.split(":") match {
      case Array(host, port) => (host, Some(port.toInt))
      case Array(host) => (host, None)
      case _ => throw new Exception(
        s"Malformed executor host string? ${executorAdded.executorInfo.executorHost}"
      )
    }

    db.findAndUpsertOne(
      getExecutor(executorAdded.executorId)
        .findAndModify(_.host setTo host)
        .and(_.port setTo portOpt)
        .and(_.time.sub.field(_.start) setTo executorAdded.time)
        .and(_.totalCores setTo executorAdded.executorInfo.totalCores)
        .and(_.logUrlMap setTo executorAdded.executorInfo.logUrlMap)
    )
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    db.findAndUpdateOne(
      getExecutor(executorRemoved.executorId)
        .findAndModify(_.time.sub.field(_.end) setTo executorRemoved.time)
        .and(_.removedReason setTo executorRemoved.reason)
    )
  }
}
