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

import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.IWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronTrigger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Local legacy coordinator. Only wrapper exit releases running resources. */
@ManagedResource(objectName = "backend:name=taskManager", description = "TaskManager LRHarvester")
public class TaskManager {
    private static final Logger logger = LogManager.getLogger(TaskManager.class);
    public enum WorkerLaunchResult { RUNNING, QUEUED, REJECTED }
    public enum ExecutionState { QUEUED, DISPATCHED, RUNNING, CANCEL_REQUESTED, COMPLETED, FAILED, CANCELLED }

    public record ExecutionSnapshot(String executionId, String groupId, String contextId, Long serialLaneId,
            String workerName, Long networkId, String networkAcronym, ExecutionState state, String waitingReason,
            Instant admittedAt, Instant startedAt, Instant finishedAt, String failure) { }

    private final Object lock = new Object();
    private final TaskScheduler scheduler;
    private final ExecutorService executor;
    private volatile RuntimeConfiguration configuration;
    private final Clock clock;
    // Insertion order supplies round-robin turns between eligible contexts.
    private final LinkedHashMap<String, ArrayDeque<Execution>> pending = new LinkedHashMap<>();
    private final LinkedHashMap<String, Execution> active = new LinkedHashMap<>();
    private final Set<Long> busyLanes = new HashSet<>();
    private final ArrayDeque<Execution> ready = new ArrayDeque<>();
    private final Map<String, Execution> live = new LinkedHashMap<>();
    private final IdentityHashMap<IWorker<?>, Execution> liveWorkers = new IdentityHashMap<>();
    private final LinkedHashMap<String, ExecutionSnapshot> completed = new LinkedHashMap<>();
    private final Map<String, Schedule> schedules = new HashMap<>();
    private boolean accepting = true;
    private boolean draining;
    private int queuedCount;
    private long rejectedCount;
    private long skippedCronCount;

    public TaskManager(TaskScheduler scheduler, int maxConcurrentWorkers, int maxQueuedWorkers) {
        this(scheduler, newWorkerExecutor(maxConcurrentWorkers), maxConcurrentWorkers, maxQueuedWorkers,
                Duration.ofHours(1), 1000, Duration.ofSeconds(30), Clock.systemUTC());
    }

    public TaskManager(TaskScheduler scheduler, ExecutorService executor, int maxConcurrentWorkers,
            int maxQueuedWorkers, Duration retention, int maxRetainedExecutions, Duration shutdownTimeout, Clock clock) {
        if (maxConcurrentWorkers < 1 || maxQueuedWorkers < 0 || maxRetainedExecutions < 1
                || retention.isNegative() || retention.isZero() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("Invalid TaskManager limits or retention/shutdown durations");
        }
        this.scheduler = Objects.requireNonNull(scheduler);
        this.executor = Objects.requireNonNull(executor);
        this.configuration = new RuntimeConfiguration(maxConcurrentWorkers, maxQueuedWorkers,
                retention.getSeconds(), maxRetainedExecutions, shutdownTimeout.getSeconds());
        this.clock = Objects.requireNonNull(clock);
        logger.info("Legacy TaskManager: {} worker slots, {} pending slots, {} retained results",
                maxConcurrentWorkers, maxQueuedWorkers, maxRetainedExecutions);
    }

    static ExecutorService newWorkerExecutor(int size) {
        if (size < 1) throw new IllegalArgumentException("taskmanager.concurrent.tasks must be positive");
        AtomicInteger number = new AtomicInteger();
        return new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(size),
                task -> new Thread(task, "taskWorker-" + number.incrementAndGet()), new ThreadPoolExecutor.AbortPolicy());
    }

    public record RuntimeConfiguration(int concurrentTasks, int maxQueuedTasks, long resultRetentionSeconds,
            int maxRetainedResults, long shutdownTimeoutSeconds) {
        public RuntimeConfiguration {
            if (concurrentTasks < 1 || maxQueuedTasks < 0 || resultRetentionSeconds < 1
                    || maxRetainedResults < 1 || shutdownTimeoutSeconds < 0) {
                throw new IllegalArgumentException("Invalid TaskManager limits or retention/shutdown durations");
            }
            try {
                Instant.now().minusSeconds(resultRetentionSeconds);
                Duration.ofSeconds(shutdownTimeoutSeconds).toMillis();
            } catch (java.time.DateTimeException | ArithmeticException failure) {
                throw new IllegalArgumentException("Retention or shutdown duration exceeds the supported range", failure);
            }
        }
    }

    public RuntimeConfiguration getRuntimeConfiguration() { return configuration; }

    /** Lower limits preserve admitted work; they constrain subsequent reservations and admissions. */
    public void updateRuntimeConfiguration(RuntimeConfiguration value) {
        Objects.requireNonNull(value);
        synchronized (lock) {
            if (!accepting || executor.isShutdown()) throw new TaskSubmissionRejectedException("TASKMANAGER_SHUTDOWN");
            if (!(executor instanceof ThreadPoolExecutor pool)) {
                throw new IllegalStateException("Worker executor cannot be resized");
            }
            int size = value.concurrentTasks();
            if (size > pool.getMaximumPoolSize()) {
                pool.setMaximumPoolSize(size);
                pool.setCorePoolSize(size);
            } else {
                pool.setCorePoolSize(size);
                pool.setMaximumPoolSize(size);
            }
            configuration = value;
            purgeCompleted();
        }
        logger.info("TaskManager runtime configuration applied: {}", value);
        drain();
    }

    /** Validate and publish an entire group before any member can enter its body. */
    public TaskSubmission submitWorkers(List<IWorker<?>> workers) {
        return submitWorkers(workers, null);
    }

    private TaskSubmission submitWorkers(List<IWorker<?>> workers, Schedule origin) {
        Objects.requireNonNull(workers, "workers");
        String groupId = UUID.randomUUID().toString();
        List<Execution> batch = new ArrayList<>();
        Set<IWorker<?>> unique = Collections.newSetFromMap(new IdentityHashMap<>());
        for (IWorker<?> worker : List.copyOf(workers)) {
            if (!unique.add(worker) || worker.getScheduledFuture() != null) {
                throw new IllegalArgumentException("Each submission needs a fresh worker instance");
            }
            batch.add(new Execution(groupId, worker, origin));
        }
        TaskSubmission submission;
        synchronized (lock) {
            if (!accepting || executor.isShutdown()) return reject(groupId, "TASKMANAGER_SHUTDOWN");
            if (origin != null && schedules.get(origin.contextId) != origin) return reject(groupId, "SCHEDULE_REPLACED");
            if (batch.stream().anyMatch(e -> liveWorkers.containsKey(e.worker) || e.worker.getScheduledFuture() != null)) {
                throw new IllegalArgumentException("Worker already admitted");
            }
            int waiting = queuedCount + batch.size() - immediatelyEligible(batch);
            if (waiting > configuration.maxQueuedTasks()) return reject(groupId, "TASK_QUEUE_FULL");
            // Attach all adapters before publishing. A custom setter failure cannot partially admit the plan.
            List<Execution> attached = new ArrayList<>();
            try {
                for (Execution execution : batch) {
                    execution.worker.setScheduledFuture(execution.future);
                    attached.add(execution);
                }
            } catch (RuntimeException failure) {
                attached.forEach(e -> e.worker.setScheduledFuture(null));
                throw failure;
            }
            if (origin != null) origin.outstanding = batch.size();
            for (Execution execution : batch) {
                pending.computeIfAbsent(execution.contextId, key -> new ArrayDeque<>()).addLast(execution);
                live.put(execution.id, execution);
                liveWorkers.put(execution.worker, execution);
                queuedCount++;
            }
            reserveReady();
            submission = new TaskSubmission(groupId, true, batch.stream().map(e -> e.id).toList(),
                    batch.stream().map(e -> e.state == ExecutionState.QUEUED
                            ? WorkerLaunchResult.QUEUED : WorkerLaunchResult.RUNNING).toList(), null);
        }
        drain();
        return submission;
    }

    private TaskSubmission reject(String groupId, String reason) {
        rejectedCount++;
        logger.warn("Legacy submission {} rejected: {}", groupId, reason);
        return new TaskSubmission(groupId, false, List.of(), List.of(), reason);
    }

    // Simulates the same head-only reservation policy, including existing waiting contexts.
    private int immediatelyEligible(List<Execution> batch) {
        LinkedHashMap<String, Execution> heads = new LinkedHashMap<>();
        pending.forEach((context, queue) -> heads.put(context, queue.peekFirst()));
        batch.forEach(e -> heads.putIfAbsent(e.contextId, e));
        Set<Long> lanes = new HashSet<>(busyLanes);
        int slots = Math.max(0, configuration.concurrentTasks() - active.size());
        int count = 0;
        for (Execution head : heads.values()) {
            if (count == slots) break;
            if (!active.containsKey(head.contextId) && (head.lane < 0 || !lanes.contains(head.lane))) {
                if (head.lane >= 0) lanes.add(head.lane);
                count++;
            }
        }
        return count;
    }

    /** Must hold lock. A blocked head stays at its position. */
    private void reserveReady() {
        while (accepting && active.size() < configuration.concurrentTasks()) {
            Execution selected = null;
            Iterator<Map.Entry<String, ArrayDeque<Execution>>> iterator = pending.entrySet().iterator();
            ArrayDeque<Execution> remainder = null;
            while (iterator.hasNext()) {
                var entry = iterator.next();
                Execution head = entry.getValue().peekFirst();
                if (active.containsKey(head.contextId) || head.lane >= 0 && busyLanes.contains(head.lane)) continue;
                selected = entry.getValue().removeFirst();
                remainder = entry.getValue();
                iterator.remove();
                break;
            }
            if (selected == null) return;
            if (!remainder.isEmpty()) pending.put(selected.contextId, remainder);
            queuedCount--;
            selected.state = ExecutionState.DISPATCHED;
            active.put(selected.contextId, selected);
            if (selected.lane >= 0) busyLanes.add(selected.lane);
            ready.addLast(selected);
        }
    }

    /** Single dispatcher; executor callbacks and I/O never run under the coordinator lock. */
    private void drain() {
        synchronized (lock) {
            if (draining || !accepting) return;
            draining = true;
        }
        while (true) {
            Execution execution;
            synchronized (lock) {
                reserveReady();
                execution = ready.pollFirst();
                if (execution == null || !accepting) {
                    draining = false;
                    return;
                }
                execution.executorUnavailable = false;
            }
            try {
                executor.execute(() -> runExecution(execution));
            } catch (RejectedExecutionException failure) {
                synchronized (lock) {
                    if (live.containsKey(execution.id)) {
                        if (executor.isShutdown()) {
                            finish(execution, failure);
                        } else {
                            // Keep bounded DISPATCHED capacity until transfer can be retried; never lose an admitted task.
                            execution.executorUnavailable = true;
                            ready.addFirst(execution);
                        }
                    }
                    draining = false;
                }
                logger.warn("Worker executor rejected transfer for {}", execution.id, failure);
                return;
            } catch (RuntimeException | Error failure) {
                synchronized (lock) {
                    if (live.containsKey(execution.id)) finish(execution, failure);
                    draining = false;
                }
                throw failure;
            }
        }
    }

    private void runExecution(Execution execution) {
        synchronized (lock) {
            // Cancellation before entry is atomic with this transition; no cancelled body can start later.
            if (execution.state != ExecutionState.DISPATCHED || !live.containsKey(execution.id)) return;
            execution.state = ExecutionState.RUNNING;
            execution.startedAt = clock.instant();
            execution.runner = Thread.currentThread();
        }
        Throwable failure = null;
        try {
            execution.worker.run();
        } catch (Throwable error) {
            failure = error;
            logger.error("Worker execution {} failed", execution.id, error);
        } finally {
            synchronized (lock) {
                execution.runner = null;
                execution.bodyExited = true;
                execution.failure = failure;
                if (execution.stopHooks == 0) finish(execution, failure);
            }
            drain();
        }
    }

    /** Finish only after actual body exit and any cancellation hook, or proven prevention of entry. */
    private void finish(Execution execution, Throwable failure) {
        if (!live.containsKey(execution.id)) return;
        String reported = execution.worker instanceof BaseWorker<?> worker ? worker.getExecutionFailure() : null;
        execution.failureMessage = failure != null ? failure.toString() : reported;
        execution.state = execution.failureMessage != null ? ExecutionState.FAILED
                : execution.cancelRequested ? ExecutionState.CANCELLED : ExecutionState.COMPLETED;
        execution.finishedAt = clock.instant();
        boolean reserved = active.remove(execution.contextId, execution);
        if (reserved && execution.lane >= 0) busyLanes.remove(execution.lane);
        if (reserved) {
            ArrayDeque<Execution> ownPending = pending.remove(execution.contextId);
            if (ownPending != null) pending.put(execution.contextId, ownPending);
        }
        ready.remove(execution);
        live.remove(execution.id);
        liveWorkers.remove(execution.worker);
        if (execution.origin != null && --execution.origin.outstanding == 0) execution.origin.inFlight = false;
        completed.put(execution.id, snapshot(execution));
        purgeCompleted();
        if (failure != null) execution.future.completion.completeExceptionally(failure);
        else if (reported != null) execution.future.completion.completeExceptionally(new IllegalStateException(reported));
        else execution.future.completion.complete(null);
    }

    private boolean cancel(Execution execution, boolean interrupt, boolean runStopHook) {
        boolean invokeStop = false;
        synchronized (lock) {
            if (!live.containsKey(execution.id) || execution.cancelRequested) return false;
            execution.cancelRequested = true;
            execution.future.completion.cancel(false);
            if (execution.state == ExecutionState.QUEUED) {
                ArrayDeque<Execution> queue = pending.get(execution.contextId);
                queue.remove(execution);
                if (queue.isEmpty()) pending.remove(execution.contextId);
                queuedCount--;
                finish(execution, null);
            } else if (execution.state == ExecutionState.DISPATCHED) {
                finish(execution, null);
            } else {
                execution.state = ExecutionState.CANCEL_REQUESTED;
                if (runStopHook) {
                    execution.stopHooks++;
                    invokeStop = true;
                }
                // Interrupt while runner identity is protected, before the pool can reuse that thread.
                if (interrupt && execution.runner != null) execution.runner.interrupt();
            }
        }
        if (invokeStop) {
            try {
                execution.worker.stop();
            } catch (RuntimeException failure) {
                logger.error("Stop hook failed for {}", execution.id, failure);
            } finally {
                synchronized (lock) {
                    execution.stopHooks--;
                    if (execution.bodyExited) finish(execution, execution.failure);
                }
            }
        }
        drain();
        return true;
    }

    public WorkerLaunchResult launchWorkerWithResult(IWorker<?> worker) {
        TaskSubmission submission = submitWorkers(List.of(worker));
        return submission.accepted() ? submission.results().get(0) : WorkerLaunchResult.REJECTED;
    }

    public List<WorkerLaunchResult> launchWorkersWithResult(List<IWorker<?>> workers) {
        if (workers == null || workers.isEmpty()) return List.of();
        TaskSubmission submission = submitWorkers(workers);
        return submission.accepted() ? submission.results()
                : Collections.nCopies(workers.size(), WorkerLaunchResult.REJECTED);
    }

    public void launchWorker(IWorker<?> worker) { submitWorkers(List.of(worker)).requireAccepted(); }

    @Scheduled(fixedDelayString = "${taskmanager.reconcile.interval-ms:2000}")
    public void reconcile() {
        synchronized (lock) { purgeCompleted(); }
        drain();
    }

    private void purgeCompleted() {
        Instant oldest = clock.instant().minusSeconds(configuration.resultRetentionSeconds());
        completed.values().removeIf(snapshot -> snapshot.finishedAt().isBefore(oldest));
        while (completed.size() > configuration.maxRetainedResults()) completed.remove(completed.keySet().iterator().next());
    }

    @ManagedAttribute public int getRunningCount() { synchronized (lock) { return active.size(); } }
    @ManagedAttribute public int getQueuedCount() { synchronized (lock) { return queuedCount; } }
    @ManagedAttribute public long getRejectedCount() { synchronized (lock) { return rejectedCount; } }
    @ManagedAttribute public long getSkippedCronCount() { synchronized (lock) { return skippedCronCount; } }
    @ManagedAttribute public int getDispatchedCount() { synchronized (lock) {
        return (int) active.values().stream().filter(e -> e.state == ExecutionState.DISPATCHED).count();
    } }
    @ManagedAttribute public int getCancellationRequestedCount() { synchronized (lock) {
        return (int) active.values().stream().filter(e -> e.state == ExecutionState.CANCEL_REQUESTED).count();
    } }
    @ManagedAttribute public int getMaxConcurrentWorkers() { return configuration.concurrentTasks(); }
    @ManagedAttribute public int getMaxQueuedWorkers() { return configuration.maxQueuedTasks(); }
    public boolean isMaxConcurrentRunningWorkersReached() { return getRunningCount() >= getMaxConcurrentWorkers(); }
    public boolean isMaxQueudedWorkersReached() { return getQueuedCount() >= getMaxQueuedWorkers(); }
    public boolean isAlreadyProcesingNetwork(String id) { synchronized (lock) { return active.containsKey(id); } }
    @ManagedAttribute public Integer getActiveTaskCountByRunningContextID(String id) {
        return isAlreadyProcesingNetwork(id) ? 1 : 0;
    }

    public List<IWorker<?>> getAllRunningWorkers() {
        synchronized (lock) { return active.values().stream().<IWorker<?>>map(e -> e.worker).toList(); }
    }

    public List<ExecutionSnapshot> getExecutionSnapshots() {
        synchronized (lock) {
            List<ExecutionSnapshot> result = new ArrayList<>(completed.values());
            live.values().forEach(e -> result.add(snapshot(e)));
            return List.copyOf(result);
        }
    }

    public Optional<ExecutionSnapshot> getWorkerSnapshot(IWorker<?> worker) {
        synchronized (lock) {
            if (worker.getScheduledFuture() instanceof TaskManager.ExecutionFuture future && future.owner() == this) {
                return Optional.of(snapshot(future.execution));
            }
            return Optional.empty();
        }
    }

    public Optional<ExecutionSnapshot> getExecutionSnapshot(String id) {
        synchronized (lock) {
            Execution execution = live.get(id);
            return Optional.ofNullable(execution == null ? completed.get(id) : snapshot(execution));
        }
    }

    private ExecutionSnapshot snapshot(Execution e) {
        String reason = null;
        if (e.state == ExecutionState.QUEUED) {
            ArrayDeque<Execution> queue = pending.get(e.contextId);
            if (queue != null && queue.peekFirst() != e) reason = "CONTEXT_ORDER";
            else if (active.containsKey(e.contextId)) reason = "CONTEXT_BUSY";
            else if (e.lane >= 0 && busyLanes.contains(e.lane)) reason = "LANE_BUSY";
            else reason = "GLOBAL_CAPACITY";
        } else if (e.executorUnavailable) reason = "EXECUTOR_UNAVAILABLE";
        return new ExecutionSnapshot(e.id, e.groupId, e.contextId, e.lane, e.name, e.networkId,
                e.networkAcronym, e.state, reason, e.admittedAt, e.startedAt, e.finishedAt, e.failureMessage);
    }

    @ManagedAttribute public List<String> getRunningTasksByRunningContextID(String id) {
        IWorker<?> worker;
        ExecutionSnapshot snapshot;
        synchronized (lock) {
            Execution execution = active.get(id);
            if (execution == null) return List.of();
            worker = execution.worker;
            snapshot = snapshot(execution);
        }
        return List.of(snapshot.workerName() + ": " + snapshot.state() + " - " + worker.getStatus());
    }

    @ManagedAttribute public List<String> getQueuedTasksByRunningContextID(String id) {
        synchronized (lock) {
            ArrayDeque<Execution> queue = pending.get(id);
            if (queue == null) return List.of();
            return queue.stream().map(e -> e.name + ": QUEUED (" + snapshot(e).waitingReason() + ")").toList();
        }
    }

    public boolean killAllTaskByRunningContextID(String id) {
        Execution execution;
        synchronized (lock) { execution = active.get(id); }
        return execution != null && cancel(execution, true, true);
    }

    public void clearQueueByRunningContextID(String id) {
        synchronized (lock) { clearQueued(id); }
        drain();
    }

    private void clearQueued(String id) {
        ArrayDeque<Execution> queue = pending.remove(id);
        if (queue == null) return;
        for (Execution execution : queue) {
            queuedCount--;
            execution.cancelRequested = true;
            execution.future.completion.cancel(false);
            finish(execution, null);
        }
    }

    public void cancelAllByRunningContextID(String id) {
        Execution execution;
        synchronized (lock) {
            // Clearing pending work and selecting the active body is one operation.
            clearQueued(id);
            execution = active.get(id);
        }
        if (execution != null) cancel(execution, true, true);
        else drain();
    }

    public boolean cancelExecution(String id) {
        Execution execution;
        synchronized (lock) { execution = live.get(id); }
        return execution != null && cancel(execution, true, true);
    }

    /** Replaces one context schedule. Every firing prepares fresh workers, outside the worker pool. */
    public void scheduleWorkers(String contextId, String description, Supplier<List<IWorker<?>>> factory, String cron) {
        CronTrigger trigger = new CronTrigger(cron);
        Schedule schedule = new Schedule(contextId, description, factory);
        Schedule previous;
        synchronized (lock) {
            if (!accepting) throw new TaskSubmissionRejectedException("TASKMANAGER_SHUTDOWN");
            previous = schedules.put(contextId, schedule);
        }
        if (previous != null && previous.future != null) previous.future.cancel(false);
        try {
            ScheduledFuture<?> future = scheduler.schedule(() -> fire(schedule), trigger);
            if (future == null) throw new IllegalArgumentException("Cron has no future execution");
            synchronized (lock) {
                if (schedules.get(contextId) != schedule) future.cancel(false);
                else schedule.future = future;
            }
        } catch (RuntimeException failure) {
            synchronized (lock) { schedules.remove(contextId, schedule); }
            throw failure;
        }
    }

    private void fire(Schedule schedule) {
        synchronized (lock) {
            if (!accepting || schedules.get(schedule.contextId) != schedule) return;
            if (schedule.inFlight || live.values().stream().anyMatch(e -> e.origin != null
                    && e.origin.contextId.equals(schedule.contextId))) {
                skippedCronCount++;
                return;
            }
            schedule.inFlight = true;
        }
        try {
            TaskSubmission submission = submitWorkers(schedule.factory.get(), schedule);
            if (!submission.accepted()) logger.warn("Cron {} rejected: {}", schedule.contextId, submission.reason());
        } catch (RuntimeException failure) {
            logger.error("Cannot prepare scheduled plan for {}", schedule.contextId, failure);
        } finally {
            synchronized (lock) { if (schedule.outstanding == 0) schedule.inFlight = false; }
        }
    }

    public void clearScheduleByRunningContextID(String id) {
        Schedule schedule;
        synchronized (lock) { schedule = schedules.remove(id); }
        if (schedule != null && schedule.future != null) schedule.future.cancel(false);
    }

    @ManagedAttribute public List<String> getScheduledTasksByRunningContextID(String id) {
        Schedule schedule;
        synchronized (lock) { schedule = schedules.get(id); }
        if (schedule == null || schedule.future == null) return List.of();
        return List.of(schedule.description + " (" + schedule.future.getDelay(TimeUnit.SECONDS) + " secs.)");
    }

    @PreDestroy public void shutdown() {
        List<Schedule> cron;
        List<Execution> running;
        synchronized (lock) {
            if (!accepting) return;
            accepting = false;
            cron = List.copyOf(schedules.values());
            schedules.clear();
            for (String context : List.copyOf(pending.keySet())) clearQueued(context);
            running = List.copyOf(active.values());
        }
        cron.forEach(s -> { if (s.future != null) s.future.cancel(false); });
        running.forEach(e -> cancel(e, true, true));
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Duration.ofSeconds(configuration.shutdownTimeoutSeconds()).toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                logger.warn("Shutdown timed out; {} executions still retain their resources", getRunningCount());
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private final class Execution {
        final String id = UUID.randomUUID().toString();
        final String groupId;
        final IWorker<?> worker;
        final String contextId;
        final long lane;
        final String name;
        final Long networkId;
        final String networkAcronym;
        final Instant admittedAt = clock.instant();
        final Schedule origin;
        final ExecutionFuture future = new ExecutionFuture(this);
        ExecutionState state = ExecutionState.QUEUED;
        Instant startedAt;
        Instant finishedAt;
        Thread runner;
        boolean cancelRequested;
        boolean bodyExited;
        boolean executorUnavailable;
        int stopHooks;
        Throwable failure;
        String failureMessage;

        Execution(String groupId, IWorker<?> worker, Schedule origin) {
            this.groupId = groupId;
            this.worker = Objects.requireNonNull(worker);
            this.contextId = Objects.requireNonNull(worker.getRunningContext(), "worker context").getId();
            if (contextId == null || contextId.isBlank()) throw new IllegalArgumentException("Worker context ID is required");
            this.lane = Objects.requireNonNull(worker.getSerialLaneId(), "worker serial lane");
            this.name = worker.getName() == null ? worker.getClass().getSimpleName() : worker.getName();
            this.origin = origin;
            var network = worker.getRunningContext() instanceof NetworkRunningContext context ? context.getNetwork() : null;
            this.networkId = network == null ? null : network.getId();
            this.networkAcronym = network == null ? null : network.getAcronym();
        }
    }

    /** Compatibility future: its cancellation never releases a body which is still running. */
    private final class ExecutionFuture implements ScheduledFuture<Void> {
        final Execution execution;
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        ExecutionFuture(Execution execution) { this.execution = execution; }
        TaskManager owner() { return TaskManager.this; }
        @Override public boolean cancel(boolean interrupt) { return TaskManager.this.cancel(execution, interrupt, false); }
        @Override public boolean isCancelled() { return completion.isCancelled(); }
        @Override public boolean isDone() { return completion.isDone(); }
        @Override public Void get() throws InterruptedException, ExecutionException { return completion.get(); }
        @Override public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return completion.get(timeout, unit);
        }
        @Override public long getDelay(TimeUnit unit) { return 0; }
        @Override public int compareTo(Delayed other) { return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS)); }
    }

    private static final class Schedule {
        final String contextId;
        final String description;
        final Supplier<List<IWorker<?>>> factory;
        volatile ScheduledFuture<?> future;
        boolean inFlight;
        int outstanding;
        Schedule(String contextId, String description, Supplier<List<IWorker<?>>> factory) {
            this.contextId = Objects.requireNonNull(contextId);
            this.description = Objects.requireNonNull(description);
            this.factory = Objects.requireNonNull(factory);
        }
    }
}
