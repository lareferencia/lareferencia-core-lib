package org.lareferencia.core.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LegacyNetworkActionExecutorTest {
    private TaskManager tasks;
    private ApplicationContext context;
    private LegacyNetworkActionExecutor executor;
    private NetworkAction action;
    private Network network;

    @BeforeEach void setUp() {
        tasks = mock(TaskManager.class);
        context = mock(ApplicationContext.class);
        ApplicationActionCatalogService catalogue = mock(ApplicationActionCatalogService.class);
        NetworkActionConfigurationService configuration = mock(NetworkActionConfigurationService.class);
        WorkerConfigurationApplier applier = mock(WorkerConfigurationApplier.class);
        executor = new LegacyNetworkActionExecutor(tasks, context, mock(NetworkRepository.class), catalogue, configuration, applier);
        action = new NetworkAction();
        action.setName("EXAMPLE");
        action.setWorkers(List.of("first", "second"));
        executor.setActions(List.of(action));
        network = new Network();
        org.springframework.test.util.ReflectionTestUtils.setField(network, "id", 1L);
        network.setAcronym("TEST");
    }

    @Test void preparesEveryStepBeforePublishingTheWholePlan() {
        Worker first = new Worker();
        Worker second = new Worker();
        when(context.getBean("first")).thenReturn(first);
        when(context.getBean("second")).thenReturn(second);
        when(tasks.submitWorkers(anyList())).thenReturn(new TaskSubmission("plan", true, List.of("1", "2"),
                List.of(TaskManager.WorkerLaunchResult.RUNNING, TaskManager.WorkerLaunchResult.QUEUED), null));
        var submission = executor.submitAction("EXAMPLE", true, network);
        assertEquals("plan", submission.groupId());
        verify(tasks).submitWorkers(List.of(first, second));
        assertEquals("NETWORK::1", first.getRunningContext().getId());
        assertTrue(second.isIncremental());
    }

    @Test void preparationFailureNeverPublishesTheFirstWorker() {
        when(context.getBean("first")).thenReturn(new Worker());
        when(context.getBean("second")).thenThrow(new IllegalStateException("invalid second bean"));
        assertThrows(IllegalStateException.class, () -> executor.executeAction("EXAMPLE", false, network));
        verify(tasks, never()).submitWorkers(anyList());
    }

    @Test void queueRejectionReachesLegacyVoidCallers() {
        when(context.getBean(anyString())).thenAnswer(call -> new Worker());
        when(tasks.submitWorkers(anyList())).thenReturn(new TaskSubmission("plan", false, List.of(), List.of(), "TASK_QUEUE_FULL"));
        var failure = assertThrows(TaskSubmissionRejectedException.class,
                () -> executor.executeAction("EXAMPLE", false, network));
        assertEquals("TASK_QUEUE_FULL", failure.getReason());
    }

    @Test void cancellingNetworkUsesOneCombinedCoordinatorOperation() {
        executor.killAndUnqueueActions(network);
        verify(tasks).cancelAllByRunningContextID("NETWORK::1");
        verify(tasks, never()).clearQueueByRunningContextID(anyString());
    }

    private static class Worker extends BaseWorker<NetworkRunningContext> {
        @Override public void run() { }
    }
}
