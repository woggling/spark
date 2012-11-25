package spark.scheduler

import scala.collection.mutable.Map

import spark.TaskEndReason

/**
 * Interface for getting events back from the TaskScheduler.
 */
private[spark] trait TaskSchedulerListener {
  // A task has finished or failed.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]): Unit

  // A node was lost from the cluster.
  def executorLost(execId: String): Unit

  // The TaskScheduler wants to abort an entire task set.
  def taskSetFailed(taskSet: TaskSet, reason: String): Unit

  // We declined to schedule tasks due to locality constraints;
  // and we last launched a local task the specified number of milliseconds ago
  def localTasksNotFound(taskSet: TaskSet, taskIndexes: Seq[Int], delay: Long,
                         targetHost: String): Unit
}
