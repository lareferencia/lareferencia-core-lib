/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.task;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.worker.IWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronTrigger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Manages task scheduling, queueing, and execution for workers in the system.
 * Provides JMX monitoring capabilities and enforces concurrency limits.
 * 
 * <p>
 * This TaskManager is used in legacy mode (workflow.engine=legacy).
 * When workflow.engine=flowable, WorkflowService is used instead.
 */
@ManagedResource(objectName = "backend:name=taskManager", description = "TaskManager LRHarvester")
public class TaskManager {

	private static Logger logger = LogManager.getLogger(TaskManager.class);

	private class QueueMap<E, I> extends ConcurrentHashMap<I, ConcurrentLinkedQueue<E>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3912244083017086119L;

		public QueueMap() {
			super();
		}

		public Queue<E> getQueue(I id) {

			ConcurrentLinkedQueue<E> queue = this.get(id);
			if (queue == null) {
				queue = new ConcurrentLinkedQueue<E>();
				this.put(id, queue);
			}
			return queue;
		}

		public void enqueue(I id, E obj) {
			this.getQueue(id).add(obj);
		}

		public E poll(I id) {
			return this.getQueue(id).poll();
		}

		public boolean removeIf(I id, Predicate<E> predicate) {
			return this.getQueue(id).removeIf(predicate);
		}

		public void clearQueue(I id) {
			this.getQueue(id).clear();
		}

		public int totalSize() {
			int size = 0;
			for (ConcurrentLinkedQueue<E> q : this.values())
				size += q.size();
			return size;
		}
	}

	@Autowired
	@org.springframework.beans.factory.annotation.Qualifier("taskScheduler")
	private TaskScheduler scheduler;

	private QueueMap<ScheduledTaskLauncher, String> scheduledTasks;
	private QueueMap<IWorker<?>, String> queuedWorkers;
	private QueueMap<IWorker<?>, String> runningWorkers;

	private QueueMap<IWorker<?>, Long> serialLane;

	@Value("${taskmanager.concurrent.tasks:4}")
	private int maxConcurrentWorkers = 4;

	@Value("${taskmanager.max_queuded.tasks:32}")
	private int maxQueuedWorkers = 32;

	// private int currentRunningWorkers = 0;

	/**
	 * Constructs a new TaskManager and initializes all internal queues and maps.
	 */
	public TaskManager() {
		// scheduledTasks = new ConcurrentLinkedQueue<ScheduledTaskLauncher>();
		scheduledTasks = new QueueMap<ScheduledTaskLauncher, String>();
		queuedWorkers = new QueueMap<IWorker<?>, String>();
		runningWorkers = new QueueMap<IWorker<?>, String>();
		serialLane = new QueueMap<IWorker<?>, Long>();
		// currentRunningWorkers = 0;
	}

	/***
	 * Proyectores de información para JMX
	 */

	/**
	 * Gets the total count of running workers across all contexts.
	 *
	 * @return total number of running workers
	 */
	@ManagedAttribute
	public int getRunningCount() {
		return runningWorkers.totalSize();
	}

	/**
	 * Gets the total count of queued workers across all contexts.
	 *
	 * @return total number of queued workers
	 */
	@ManagedAttribute
	public int getQueuedCount() {
		return queuedWorkers.totalSize();
	}

	/**
	 * Gets all running workers across all contexts.
	 *
	 * @return list of all running workers
	 */
	public List<IWorker<?>> getAllRunningWorkers() {
		List<IWorker<?>> allWorkers = new ArrayList<>();
		for (ConcurrentLinkedQueue<IWorker<?>> queue : runningWorkers.values()) {
			allWorkers.addAll(queue);
		}
		return allWorkers;
	}

	/**
	 * Gets the list of running tasks for a specific running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return list of running task descriptions
	 */
	@ManagedAttribute
	public List<String> getRunningTasksByRunningContextID(String runningContextID) {

		List<String> result = new ArrayList<String>();

		for (IWorker<?> worker : runningWorkers.getQueue(runningContextID)) {

			// String running = worker.getScheduledFuture().isDone() ? "(done)" :
			// "(running)";
			result.add(worker.toString() /* + running */ );
		}
		return result;
	}

	/**
	 * Gets the list of queued tasks waiting to be executed for a specific running
	 * context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return list of queued task descriptions
	 */
	@ManagedAttribute
	public List<String> getQueuedTasksByRunningContextID(String runningContextID) {

		List<String> result = new ArrayList<String>();

		for (IWorker worker : queuedWorkers.getQueue(runningContextID)) {

			result.add(worker.toString());
		}

		return result;
	}

	/**
	 * Gets the list of scheduled tasks with their delay information for a specific
	 * running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return list of scheduled task descriptions with delay times
	 */
	@ManagedAttribute
	public List<String> getScheduledTasksByRunningContextID(String runningContextID) {

		List<String> result = new ArrayList<String>();

		for (ScheduledTaskLauncher tl : scheduledTasks.getQueue(runningContextID)) {

			Long seconds = tl.getScheduledFuture().getDelay(TimeUnit.SECONDS);
			String toRunDescription = seconds + " secs.";

			if (seconds > 3600) {
				Long hours = tl.getScheduledFuture().getDelay(TimeUnit.HOURS);
				toRunDescription = hours + " hours";

				if (hours > 72) {
					Long days = tl.getScheduledFuture().getDelay(TimeUnit.DAYS);
					toRunDescription = days + " days";
				}
			}

			result.add(tl.getWorker().toString() + "(" + toRunDescription + ")");
		}
		return result;
	}

	/**
	 * Gets the count of active tasks for a specific running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return the number of active tasks
	 */
	@ManagedAttribute
	public Integer getActiveTaskCountByRunningContextID(String runningContextID) {
		return runningWorkers.getQueue(runningContextID).size();
	}

	/**
	 * Kills all running tasks for a specific running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return true if any task was killed, false otherwise
	 */
	public synchronized boolean killAllTaskByRunningContextID(String runningContextID) {

		boolean wasKilled = false;
		for (IWorker wkr : runningWorkers.getQueue(runningContextID)) {
			// si hay procesos sobre la misma red y no estan cancelados
			if (!wkr.getScheduledFuture().isDone() && !wkr.getScheduledFuture().isCancelled()) {
				wkr.stop();
				wasKilled = true;
			}
		}

		return wasKilled;
	}

	/**
	 * Checks if the maximum number of concurrent running workers has been reached.
	 * 
	 * @return true if at maximum capacity, false otherwise
	 */
	public synchronized boolean isMaxConcurrentRunningWorkersReached() {
		return runningWorkers.totalSize() >= maxConcurrentWorkers;
	}

	/**
	 * Checks if the maximum number of queued workers has been reached.
	 * 
	 * @return true if queue is at maximum capacity, false otherwise
	 */
	public synchronized boolean isMaxQueudedWorkersReached() {
		return queuedWorkers.totalSize() >= maxQueuedWorkers;
	}

	/**
	 * Checks if there is already a task processing for the specified running
	 * context.
	 * 
	 * @param runningContextID the identifier of the running context
	 * @return true if a task is already processing, false otherwise
	 */
	public synchronized boolean isAlreadyProcesingNetwork(String runningContextID) {
		boolean alreadyProcesingThisNetwork = false;
		for (IWorker<?> wkr : runningWorkers.getQueue(runningContextID)) {
			// si hay procesos sobre la misma red y no estan cancelados
			alreadyProcesingThisNetwork |= !wkr.getScheduledFuture().isDone()
					&& !wkr.getScheduledFuture().isCancelled();
		}

		return alreadyProcesingThisNetwork;
	}

	/**
	 * Launches a worker task if resource limits and serialization constraints
	 * allow.
	 * Enqueues the worker if it cannot be launched immediately.
	 * 
	 * @param worker the worker to launch
	 */
	public synchronized void launchWorker(IWorker<?> worker) {

		String runningContextID = worker.getRunningContext().getId();
		Long serialLaneID = worker.getSerialLaneId();

		// Para lanzar un proceso se verifica que no haya otro corriendo para esa red
		// y si su serialLaneID es positivo se verifica que no haya ninguno corriendo
		// eso asegura la serialidad de los workers cuando tienen serialLanes asignadas
		if (!isMaxConcurrentRunningWorkersReached() && !isAlreadyProcesingNetwork(runningContextID)
				&& (serialLaneID < 0 || serialLane.getQueue(serialLaneID).isEmpty())) {

			logger.debug("Launching process: " + worker.toString());

			ScheduledFuture<?> sf = scheduler.schedule(worker, new Date());
			worker.setScheduledFuture(sf);

			runningWorkers.enqueue(runningContextID, worker);
			// currentRunningWorkers++;

			// se agrega al serial lane
			if (serialLaneID >= 0) {
				logger.debug("This process will run serialized in serialLaneID:" + serialLaneID);
				serialLane.enqueue(serialLaneID, worker);
			}

			logger.debug("Process queue :: " + runningWorkers);

		} else {
			if (isMaxConcurrentRunningWorkersReached())
				logger.info("Max concurrent workers limits reached: " + maxConcurrentWorkers +
						". Worker will be queued. This value can be increased in application.properties :: taskmanager.concurrent.tasks ");
			else if (serialLaneID > 0)
				logger.debug("This serial lane: " + serialLaneID + "  is already in use. Worker will be queued");
			else
				logger.debug("This network: " + runningContextID + " is already in use. Worker will be queued");

			if (!isMaxQueudedWorkersReached()) {
				queuedWorkers.enqueue(runningContextID, worker);
				logger.debug("Waiting queue :: " + queuedWorkers);
			} else
				logger.info("Waiting queue reached max allowed size: " + maxQueuedWorkers +
						". This value can be increased in application.properties, taskmanager.max_queuded.tasks  ");

		}

	}

	@Scheduled(fixedRate = 2000) // Reduced from 10000ms to 2000ms for faster cleanup
	private synchronized void cleanFinishedTasksAndRunQueued() {

		logger.debug("Cleaning finished process and running queuede ones");

		Predicate<IWorker<?>> isDoneOrCancelled = new Predicate<IWorker<?>>() {
			@Override
			public boolean test(IWorker<?> worker) {
				return (worker.getScheduledFuture().isDone() || worker.getScheduledFuture().isCancelled());
			}
		};

		logger.debug("Cleanning finished process from serialLane");
		for (Long serialLaneID : serialLane.keySet()) {
			serialLane.removeIf(serialLaneID, isDoneOrCancelled);
		}

		logger.debug("Cleaning finished process from running queue");
		for (String runningContextID : runningWorkers.keySet()) {
			// remueve de la cola los procesos terminados
			boolean removed = runningWorkers.removeIf(runningContextID, isDoneOrCancelled);
			// if ( removed )
			// currentRunningWorkers--;
		}

		logger.debug("Trying to run queued workers");
		for (String runningContextID : queuedWorkers.keySet()) {
			// luego de la limpieza trata de correr nuevos procesos (debe verificar que no
			// haya otros corriendo)
			runQueuedByRunningContextID(runningContextID);
		}

		logger.debug("Running queue :: " + runningWorkers);
	}

	private void runQueuedByRunningContextID(String runningContextID) {

		if (runningWorkers.getQueue(runningContextID).size() == 0
				&& queuedWorkers.getQueue(runningContextID).size() > 0) {

			logger.debug("Unqueue worker with contextID: " + runningContextID);
			IWorker<?> worker = queuedWorkers.poll(runningContextID);
			if (worker != null)
				launchWorker(worker);
		}

	}

	/**
	 * Clears all queued workers for a specific running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 */
	public synchronized void clearQueueByRunningContextID(String runningContextID) {
		queuedWorkers.clearQueue(runningContextID);
	}

	/**
	 * Schedules a worker to run according to a cron expression.
	 * 
	 * @param worker         the worker to schedule
	 * @param cronExpression the cron expression defining the schedule
	 */
	public synchronized void scheduleWorker(IWorker<?> worker, String cronExpression) {

		try {
			ScheduledTaskLauncher tl = new ScheduledTaskLauncher(worker, cronExpression);
			scheduledTasks.enqueue(worker.getRunningContext().getId(), tl);
		} catch (IllegalArgumentException e) {
			logger.error("Cron Expression error in : " + worker.getRunningContext().getId());
		}
	}

	/**
	 * Clears all scheduled tasks for a specific running context.
	 * 
	 * @param runningContextID the identifier of the running context
	 */
	public synchronized void clearScheduleByRunningContextID(String runningContextID) {
		for (ScheduledTaskLauncher tl : scheduledTasks.getQueue(runningContextID)) {
			logger.debug("Removing Schedule:" + tl.getWorker().toString());
			tl.getWorker().stop();
			tl.scheduledFuture.cancel(true);
			scheduledTasks.clearQueue(runningContextID);
		}
	}

	/***
	 * Esta clase es utilizada como lanzador para workers programados
	 * , se programa la ejecución del lanzador en lugar del worker y es el lanzador
	 * queien llama al worker.
	 * 
	 * @author lmatas
	 *
	 */
	class ScheduledTaskLauncher implements Runnable {

		@Getter
		IWorker<?> worker;

		@Getter
		@Setter
		ScheduledFuture<?> scheduledFuture;

		public ScheduledTaskLauncher(IWorker<?> worker, String cronExpression) {
			this.worker = worker;
			scheduledFuture = scheduler.schedule(this, new CronTrigger(cronExpression));
		}

		@Override
		public void run() {
			launchWorker(this.worker);
		}

	}

}
