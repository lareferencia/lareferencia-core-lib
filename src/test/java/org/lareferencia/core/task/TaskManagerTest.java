package org.lareferencia.core.task;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.IRunningContext;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.lareferencia.core.task.TaskManager.ExecutionState.*;

class TaskManagerTest {
    private final List<TaskManager> managers = new ArrayList<>();
    private final List<CountDownLatch> releases = new ArrayList<>();

    @AfterEach void close() {
        releases.forEach(CountDownLatch::countDown);
        managers.forEach(TaskManager::shutdown);
    }

    private TaskManager manager(ExecutorService executor, int slots, int queued) {
        return manager(mock(TaskScheduler.class), executor, slots, queued, Clock.systemUTC(), 100);
    }

    private TaskManager manager(TaskScheduler scheduler, ExecutorService executor, int slots, int queued, Clock clock, int retained) {
        TaskManager manager = new TaskManager(scheduler, executor, slots, queued,
                Duration.ofMinutes(1), retained, Duration.ofSeconds(2), clock);
        managers.add(manager);
        return manager;
    }

    private CountDownLatch release() {
        CountDownLatch latch = new CountDownLatch(1);
        releases.add(latch);
        return latch;
    }

    @Test void blockedHeadsNeverRotateAndNewRequestsCannotOvertake() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 10);
        List<String> order = new ArrayList<>();
        manager.submitWorkers(List.of(worker("other", -1, () -> order.add("blocker"))));
        manager.submitWorkers(List.of(worker("network", -1, () -> order.add("harvest")),
                worker("network", -1, () -> order.add("validate")), worker("network", -1, () -> order.add("index"))));
        for (int i = 0; i < 8; i++) manager.reconcile();
        manager.submitWorkers(List.of(worker("network", -1, () -> order.add("new"))));
        executor.runAll();
        assertEquals(List.of("blocker", "harvest", "validate", "index", "new"), order);
        assertEquals(0, manager.getQueuedCount());
        assertEquals(0, manager.getRunningCount());
    }

    @Test void laneBlockedHeadDoesNotBlockIndependentContexts() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 2, 5);
        List<String> order = new ArrayList<>();
        manager.submitWorkers(List.of(worker("holder", 1, () -> order.add("holder"))));
        manager.submitWorkers(List.of(worker("network", 1, () -> order.add("head")),
                worker("network", -1, () -> order.add("successor"))));
        manager.submitWorkers(List.of(worker("independent", 2, () -> order.add("independent"))));
        executor.runLast();
        assertEquals(List.of("independent"), order);
        assertEquals(2, manager.getQueuedCount());
        executor.runAll();
        assertEquals(List.of("independent", "holder", "head", "successor"), order);
    }

    @Test void eligibleContextsTakeTurnsRatherThanDrainingOneNetwork() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 8);
        List<String> order = new ArrayList<>();
        manager.submitWorkers(List.of(worker("holder", -1, () -> {})));
        manager.submitWorkers(List.of(worker("a", -1, () -> order.add("a1")), worker("a", -1, () -> order.add("a2"))));
        manager.submitWorkers(List.of(worker("b", -1, () -> order.add("b1")), worker("b", -1, () -> order.add("b2"))));
        executor.runAll();
        assertEquals(List.of("a1", "b1", "a2", "b2"), order);
    }

    @Test void batchUsesFreeWorkerSlotsEvenWhenWaitingCapacityIsZero() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 2, 0);
        var receipt = manager.submitWorkers(List.of(worker("a", -1, () -> {}), worker("b", -1, () -> {})));
        assertTrue(receipt.accepted());
        assertEquals(List.of(TaskManager.WorkerLaunchResult.RUNNING, TaskManager.WorkerLaunchResult.RUNNING), receipt.results());
        assertEquals(0, manager.getQueuedCount());
        executor.runAll();
    }

    @Test void oversizedPlanRejectsEveryWorkerWithoutAttachingFutures() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 0);
        TestWorker first = worker("a", -1, () -> fail("Rejected worker entered"));
        TestWorker second = worker("a", -1, () -> fail("Rejected worker entered"));
        TaskSubmission receipt = manager.submitWorkers(List.of(first, second));
        assertFalse(receipt.accepted());
        assertEquals("TASK_QUEUE_FULL", receipt.reason());
        assertNull(first.getScheduledFuture());
        assertNull(second.getScheduledFuture());
        assertEquals(0, manager.getRunningCount());
        assertTrue(executor.tasks.isEmpty());
    }

    @Test void malformedLaterWorkerCannotPartiallyAdmitEarlierOnes() {
        TaskManager manager = manager(new ManualExecutor(), 2, 5);
        TestWorker first = worker("a", -1, () -> {});
        TestWorker invalid = worker("", -1, () -> {});
        assertThrows(IllegalArgumentException.class, () -> manager.submitWorkers(List.of(first, invalid)));
        assertNull(first.getScheduledFuture());
        assertEquals(0, manager.getRunningCount());
    }

    @Test void transientExecutorRejectionKeepsReservationAndRetriesExactlyOnce() {
        ManualExecutor executor = new ManualExecutor();
        executor.rejectNext = true;
        TaskManager manager = manager(executor, 1, 0);
        List<String> work = new ArrayList<>();
        var receipt = manager.submitWorkers(List.of(worker("a", 4, () -> work.add("done"))));
        assertTrue(receipt.accepted());
        assertEquals(DISPATCHED, manager.getExecutionSnapshot(receipt.executionIds().get(0)).orElseThrow().state());
        assertEquals("EXECUTOR_UNAVAILABLE", manager.getExecutionSnapshot(receipt.executionIds().get(0)).orElseThrow().waitingReason());
        manager.reconcile();
        executor.runAll();
        assertEquals(List.of("done"), work);
        assertEquals(0, manager.getRunningCount());
    }

    @Test void cancellationBeforeBodyEntryPreventsExecutionAndReleasesSlot() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 0);
        TestWorker cancelled = worker("a", 1, () -> fail("Cancelled body entered"));
        var receipt = manager.submitWorkers(List.of(cancelled));
        assertTrue(manager.cancelExecution(receipt.executionIds().get(0)));
        TestWorker next = worker("a", 1, () -> {});
        assertTrue(manager.submitWorkers(List.of(next)).accepted());
        executor.runAll();
        assertEquals(CANCELLED, manager.getWorkerSnapshot(cancelled).orElseThrow().state());
        assertEquals(COMPLETED, manager.getWorkerSnapshot(next).orElseThrow().state());
    }

    @Test void cancelledFutureDoesNotReleaseContextOrLaneWhileBodyIsAlive() throws Exception {
        TaskManager manager = manager(TaskManager.newWorkerExecutor(2), 2, 5);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exit = release();
        TestWorker running = worker("a", 7, () -> { entered.countDown(); awaitIgnoringInterrupt(exit); });
        manager.submitWorkers(List.of(running));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        CountDownLatch nextEntered = new CountDownLatch(1);
        TestWorker next = worker("a", 7, nextEntered::countDown);
        TestWorker otherLaneUser = worker("b", 7, () -> {});
        manager.submitWorkers(List.of(next, otherLaneUser));
        assertTrue(manager.killAllTaskByRunningContextID("a"));
        assertTrue(running.getScheduledFuture().isCancelled());
        assertEquals(CANCEL_REQUESTED, manager.getWorkerSnapshot(running).orElseThrow().state());
        manager.reconcile();
        assertEquals(1, nextEntered.getCount());
        assertEquals(2, manager.getQueuedCount());
        TestWorker independent = worker("c", -1, () -> {});
        manager.submitWorkers(List.of(independent));
        independent.getScheduledFuture().get(3, TimeUnit.SECONDS);
        assertEquals(1, nextEntered.getCount());
        exit.countDown();
        next.getScheduledFuture().get(3, TimeUnit.SECONDS);
        otherLaneUser.getScheduledFuture().get(3, TimeUnit.SECONDS);
        assertEquals(CANCELLED, manager.getWorkerSnapshot(running).orElseThrow().state());
    }

    @Test void cancellationHookAlsoMustExitBeforeReleasingResources() throws Exception {
        TaskManager manager = manager(TaskManager.newWorkerExecutor(2), 2, 5);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch bodyExited = new CountDownLatch(1);
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch hookExit = release();
        TestWorker running = new TestWorker("a", 1, () -> {
            entered.countDown();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException expected) { bodyExited.countDown(); }
        }) {
            @Override public void stop() { hookEntered.countDown(); awaitIgnoringInterrupt(hookExit); super.stop(); }
        };
        manager.submitWorkers(List.of(running));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        var cancellation = CompletableFuture.supplyAsync(() -> manager.killAllTaskByRunningContextID("a"));
        assertTrue(hookEntered.await(3, TimeUnit.SECONDS));
        assertTrue(bodyExited.await(3, TimeUnit.SECONDS));
        TestWorker next = worker("b", 1, () -> {});
        assertEquals(List.of(TaskManager.WorkerLaunchResult.QUEUED), manager.submitWorkers(List.of(next)).results());
        assertEquals(CANCEL_REQUESTED, manager.getWorkerSnapshot(running).orElseThrow().state());
        hookExit.countDown();
        assertTrue(cancellation.get(3, TimeUnit.SECONDS));
        next.getScheduledFuture().get(3, TimeUnit.SECONDS);
    }

    @Test void clearingQueuedTasksCannotReleaseAnotherExecutionsLane() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 2, 5);
        manager.submitWorkers(List.of(worker("holder", 1, () -> {})));
        manager.submitWorkers(List.of(worker("to-clear", 1, () -> {})));
        manager.clearQueueByRunningContextID("to-clear");
        TestWorker next = worker("other", 1, () -> {});
        assertEquals(List.of(TaskManager.WorkerLaunchResult.QUEUED), manager.submitWorkers(List.of(next)).results());
        executor.runAll();
    }

    @Test void contextCancellationRemovesTheEntireRemainingPlan() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 5);
        manager.submitWorkers(List.of(worker("a", -1, () -> fail("Cancelled head entered")),
                worker("a", -1, () -> fail("Cancelled successor entered"))));
        manager.cancelAllByRunningContextID("a");
        executor.runAll();
        assertEquals(0, manager.getRunningCount());
        assertEquals(0, manager.getQueuedCount());
        assertTrue(manager.getExecutionSnapshots().stream().allMatch(e -> e.state() == CANCELLED));
    }

    @Test void monitorQueriesDoNotCreateStateAndRetentionDropsCompletedWorkers() {
        ManualExecutor executor = new ManualExecutor();
        MutableClock clock = new MutableClock();
        TaskManager manager = manager(mock(TaskScheduler.class), executor, 1, 5, clock, 2);
        for (int i = 0; i < 100; i++) {
            assertTrue(manager.getQueuedTasksByRunningContextID("unknown-" + i).isEmpty());
            assertTrue(manager.getRunningTasksByRunningContextID("unknown-" + i).isEmpty());
        }
        assertTrue(manager.getExecutionSnapshots().isEmpty());
        for (int i = 0; i < 3; i++) {
            manager.submitWorkers(List.of(worker("a", -1, () -> {})));
            executor.runAll();
        }
        assertEquals(2, manager.getExecutionSnapshots().size());
        clock.now.set(clock.instant().plusSeconds(61));
        manager.reconcile();
        assertTrue(manager.getExecutionSnapshots().isEmpty());
    }

    @Test void reportsWorkerFailuresWhichWereCaughtInsideTheBody() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 5);
        TestWorker worker = new TestWorker("a", -1, () -> {}) {
            @Override public void run() { recordExecutionFailure("snapshot failed"); }
        };
        manager.submitWorkers(List.of(worker));
        executor.runAll();
        assertEquals(FAILED, manager.getWorkerSnapshot(worker).orElseThrow().state());
        assertEquals("snapshot failed", manager.getWorkerSnapshot(worker).orElseThrow().failure());
        assertThrows(ExecutionException.class, () -> worker.getScheduledFuture().get());
    }

    @Test void cronUsesFreshInstancesSkipsDuplicatesAndReschedulingKeepsCurrentPlan() {
        TaskScheduler scheduler = mock(TaskScheduler.class);
        List<Runnable> callbacks = new ArrayList<>();
        List<ScheduledFuture<?>> futures = new ArrayList<>();
        doAnswer(call -> {
            callbacks.add(call.getArgument(0));
            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            futures.add(future);
            return future;
        }).when(scheduler).schedule(any(Runnable.class), any(Trigger.class));
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(scheduler, executor, 1, 5, Clock.systemUTC(), 100);
        List<TestWorker> workers = new ArrayList<>();
        java.util.function.Supplier<List<org.lareferencia.core.worker.IWorker<?>>> factory = () -> {
            TestWorker fresh = worker("network", -1, () -> {});
            workers.add(fresh);
            return List.of(fresh);
        };
        manager.scheduleWorkers("network", "AllActions", factory, "0 * * * * *");
        callbacks.get(0).run();
        callbacks.get(0).run();
        assertEquals(1, workers.size());
        assertEquals(1, manager.getSkippedCronCount());
        manager.scheduleWorkers("network", "AllActions", factory, "0 */2 * * * *");
        verify(futures.get(0)).cancel(false);
        callbacks.get(1).run();
        assertEquals(1, workers.size());
        assertFalse(workers.get(0).getScheduledFuture().isCancelled());
        executor.runAll();
        callbacks.get(1).run();
        assertEquals(2, workers.size());
        assertNotSame(workers.get(0), workers.get(1));
        manager.clearScheduleByRunningContextID("network");
        assertFalse(workers.get(1).getScheduledFuture().isCancelled());
        callbacks.get(1).run();
        assertEquals(2, workers.size());
        executor.runAll();
    }

    @Test void shutdownCancelsWaitingWorkAndRejectsNewPlans() {
        ManualExecutor executor = new ManualExecutor();
        TaskManager manager = manager(executor, 1, 5);
        manager.submitWorkers(List.of(worker("a", -1, () -> fail()), worker("a", -1, () -> fail())));
        manager.shutdown();
        assertEquals(0, manager.getRunningCount());
        assertEquals(0, manager.getQueuedCount());
        assertEquals("TASKMANAGER_SHUTDOWN", manager.submitWorkers(List.of(worker("a", -1, () -> {}))).reason());
        executor.runAll();
    }

    private static TestWorker worker(String context, long lane, Runnable body) { return new TestWorker(context, lane, body); }
    private static class TestWorker extends BaseWorker<IRunningContext> {
        private final Runnable body;
        TestWorker(String context, long lane, Runnable body) { super(() -> context); setSerialLaneId(lane); this.body = body; }
        @Override public void run() { body.run(); }
    }
    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try { latch.await(); break; }
            catch (InterruptedException expected) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
    private static class ManualExecutor extends AbstractExecutorService {
        final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        boolean closed;
        boolean rejectNext;
        @Override public void execute(Runnable task) {
            if (closed || rejectNext) { rejectNext = false; throw new RejectedExecutionException("test rejection"); }
            tasks.addLast(task);
        }
        void runAll() { while (!tasks.isEmpty()) tasks.removeFirst().run(); }
        void runLast() { tasks.removeLast().run(); }
        @Override public void shutdown() { closed = true; }
        @Override public List<Runnable> shutdownNow() { closed = true; return List.copyOf(tasks); }
        @Override public boolean isShutdown() { return closed; }
        @Override public boolean isTerminated() { return closed; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return closed; }
    }
    private static class MutableClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-02T00:00:00Z"));
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    }
}
