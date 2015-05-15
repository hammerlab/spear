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

    if (executorMetricsUpdate.taskMetrics.size > 0) {

      // NOTE(ryan): important to do this *before* updating Task records below :-\
      val metricsDeltas: Map[TaskID, TaskMetrics] = getTaskMetricsDeltasMap(executorMetricsUpdate.taskMetrics)

      // Update Task records
      executorMetricsUpdate.taskMetrics.map {
        case (taskId, stageId, stageAttempt, taskMetrics) =>
          db.findAndUpdateOne(
            Q(Task)
            .where(_.id eqs taskId)
            .findAndModify(_.metrics push SparkIDL.taskMetrics(taskMetrics))
          )
      }

      // Update Executor metrics
      updateExecutorMetrics(
        executorMetricsUpdate.execId,
        metricsDeltas
      )

      // Update Stage metrics
      updateStageMetrics(
        executorMetricsUpdate.taskMetrics,
        metricsDeltas
      )
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
      Q(Executor)
        .where(_.id eqs executorAdded.executorId)
        .findAndModify(_.host setTo host)
        .and(_.port setTo portOpt)
        .and(_.addedAt setTo executorAdded.time)
        .and(_.totalCores setTo executorAdded.executorInfo.totalCores)
        .and(_.logUrlMap setTo executorAdded.executorInfo.logUrlMap)
    )
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    db.findAndUpdateOne(
      Q(Executor)
      .where(_.id eqs executorRemoved.executorId)
      .findAndModify(_.removedAt setTo executorRemoved.time)
      .and(_.removedReason setTo executorRemoved.reason)
    )
  }
}
