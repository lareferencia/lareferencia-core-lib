/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

package org.lareferencia.core.flowable;

import lombok.Builder;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flowable.engine.HistoryService;
import org.flowable.engine.ManagementService;
import org.flowable.engine.RepositoryService;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.history.HistoricProcessInstance;
import org.flowable.engine.repository.ProcessDefinition;
import org.flowable.engine.runtime.Execution;
import org.flowable.engine.runtime.ProcessInstance;
import org.lareferencia.core.flowable.config.WorkflowProperties;
import org.lareferencia.core.flowable.dto.WorkflowDefinitionInfo;
import org.lareferencia.core.flowable.dto.ProcessInstanceInfo;
import org.lareferencia.core.flowable.dto.ScheduledProcessInfo;
import org.lareferencia.core.flowable.exception.QueueFullException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Service for managing Flowable BPMN process instances with concurrency
 * control.
 * <p>
 * Provides lane-based queuing where processes with the same laneId run
 * serially,
 * while processes in different lanes can run in parallel.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Service
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class WorkflowService {

    private static final Logger logger = LogManager.getLogger(WorkflowService.class);

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private RepositoryService repositoryService;

    @Autowired
    private HistoryService historyService;

    @Autowired
    private ManagementService managementService;

    @Autowired
    private WorkflowProperties config;

    // ========== Concurrency Control State ==========

    /** Tracks running process by lane: laneId -> processInstanceId */
    private final Map<String, String> runningByLane = new ConcurrentHashMap<>();

    /** Queued processes by lane */
    private final Map<String, Queue<PendingProcess>> queuedByLane = new ConcurrentHashMap<>();

    /** Track process states for cleanup */
    private final Map<String, ProcessState> processStates = new ConcurrentHashMap<>();

    /** Callbacks for worker termination - called when process is terminated */
    private final Map<String, Runnable> terminationCallbacks = new ConcurrentHashMap<>();

    /** Callbacks for worker status - queried to get current worker status */
    private final Map<String, Supplier<String>> statusCallbacks = new ConcurrentHashMap<>();

    // ========== Scheduling State ==========

    @Autowired
    @Qualifier("taskScheduler")
    private TaskScheduler taskScheduler;

    /** Active scheduled tasks: scheduleId -> ScheduledTask */
    private final Map<String, ScheduledTask> scheduledTasks = new ConcurrentHashMap<>();

    // ========== Initialization ==========

    /**
     * Recover running processes state from Flowable on startup.
     * Reconstructs internal tracking maps from persisted process instances.
     */
    @PostConstruct
    public void recoverOnStartup() {
        logger.info("WorkflowService initializing - recovering running processes from Flowable");

        try {
            // Clean up dead letter jobs (failed jobs from previous run)
            long deletedDeadLetters = managementService.createDeadLetterJobQuery()
                    .list()
                    .stream()
                    .peek(deadLetter -> {
                        logger.warn("Removing dead letter job from previous run: {} (process: {})",
                                deadLetter.getId(), deadLetter.getProcessInstanceId());
                        managementService.deleteDeadLetterJob(deadLetter.getId());
                    })
                    .count();

            if (deletedDeadLetters > 0) {
                logger.info("Cleaned up {} dead letter jobs", deletedDeadLetters);
            }

            // Recover all active process instances
            List<ProcessInstance> activeProcesses = runtimeService.createProcessInstanceQuery()
                    .active()
                    .list();

            int recoveredCount = 0;
            for (ProcessInstance process : activeProcesses) {
                try {
                    Map<String, Object> vars = runtimeService.getVariables(process.getId());
                    String laneId = (String) vars.get("laneId");

                    if (laneId != null && !laneId.isBlank()) {
                        // Reconstruct runningByLane
                        runningByLane.put(laneId, process.getId());

                        // Reconstruct processStates
                        processStates.put(process.getId(), ProcessState.builder()
                                .processInstanceId(process.getId())
                                .laneId(laneId)
                                .build());

                        recoveredCount++;
                        logger.info("Recovered running process: {} (definition: {}, lane: '{}')",
                                process.getId(), process.getProcessDefinitionKey(), laneId);
                    } else {
                        logger.warn("Process {} has no laneId, skipping recovery tracking",
                                process.getId());
                    }
                } catch (Exception e) {
                    logger.error("Error recovering process {}: {}", process.getId(), e.getMessage());
                }
            }

            // Log pending jobs that will be re-executed
            long pendingJobs = managementService.createJobQuery().count();
            long pendingTimers = managementService.createTimerJobQuery().count();

            logger.info("Startup recovery completed - {} processes recovered, {} pending jobs, {} pending timers",
                    recoveredCount, pendingJobs, pendingTimers);

            if (pendingJobs > 0) {
                logger.info("Flowable will automatically retry pending async jobs");
            }

        } catch (Exception e) {
            logger.error("Error during startup recovery: {}", e.getMessage(), e);
        }
    }

    /**
     * Graceful shutdown - terminate all running processes and clear queues.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("WorkflowService shutting down - terminating all running processes");

        // Cancel all scheduled tasks
        for (ScheduledTask task : scheduledTasks.values()) {
            if (task.future != null) {
                task.future.cancel(false);
            }
        }
        scheduledTasks.clear();
        logger.info("Cancelled all scheduled tasks");

        // Clear all queues first to prevent new processes from starting
        queuedByLane.clear();

        // Call termination callbacks for all running processes
        int terminatedCount = 0;
        for (Map.Entry<String, Runnable> entry : terminationCallbacks.entrySet()) {
            try {
                logger.debug("Calling termination callback for process: {}", entry.getKey());
                entry.getValue().run();
                terminatedCount++;
            } catch (Exception e) {
                logger.warn("Error calling termination callback for {}: {}", entry.getKey(), e.getMessage());
            }
        }

        // Wait for workers to finish processing after receiving stop signal
        if (terminatedCount > 0) {
            logger.info("Waiting for {} workers to finish gracefully...", terminatedCount);
            try {
                // Give workers time to finish their current batch and close resources
                Thread.sleep(5000); // 5 seconds should be enough for most cases
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for workers to finish");
                Thread.currentThread().interrupt();
            }
            logger.info("Wait complete - proceeding with shutdown");
        }

        terminationCallbacks.clear();
        statusCallbacks.clear();

        // Delete all running process instances from Flowable
        try {
            List<ProcessInstance> runningProcesses = runtimeService.createProcessInstanceQuery().list();
            for (ProcessInstance process : runningProcesses) {
                try {
                    runtimeService.deleteProcessInstance(process.getId(), "Application shutdown");
                    terminatedCount++;
                } catch (Exception e) {
                    logger.debug("Could not delete process {}: {}", process.getId(), e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.debug("Could not query running processes during shutdown: {}", e.getMessage());
        }

        // Clear internal tracking state
        runningByLane.clear();
        processStates.clear();

        logger.info("WorkflowService shutdown completed - terminated {} processes", terminatedCount);
    }

    // ========== Submit Process (with queuing) ==========

    /**
     * Submits a process for execution. If the lane is busy, the process is queued.
     * 
     * @param processKey the process definition key
     * @param variables  the process variables (must include laneId)
     * @return info about the started or queued process
     * @throws IllegalArgumentException if laneId is missing
     * @throws QueueFullException       if the queue is at maximum capacity
     */
    public synchronized ProcessInstanceInfo submitProcess(String processKey, Map<String, Object> variables) {
        String laneId = (String) variables.get("laneId");

        if (laneId == null || laneId.isBlank()) {
            throw new IllegalArgumentException("'laneId' is required in process variables");
        }

        logger.info("Submitting process '{}' for lane '{}'", processKey, laneId);

        if (isLaneAvailable(laneId)) {
            return launchProcess(processKey, variables, laneId);
        } else {
            // Check queue limits
            if (getTotalQueuedCount() >= config.getMaxQueuedProcesses()) {
                throw new QueueFullException("Max queued processes reached: " + config.getMaxQueuedProcesses());
            }
            if (getQueuedCountForLane(laneId) >= config.getMaxQueuedPerLane()) {
                throw new QueueFullException("Max queued for lane '" + laneId + "': " + config.getMaxQueuedPerLane());
            }

            enqueueForLane(processKey, variables, laneId);
            logger.info("Process '{}' queued for lane '{}'", processKey, laneId);

            return ProcessInstanceInfo.builder()
                    .processDefinitionKey(processKey)
                    .variables(variables)
                    .completed(false)
                    .suspended(false)
                    .build();
        }
    }

    private ProcessInstanceInfo launchProcess(String processKey, Map<String, Object> variables, String laneId) {
        logger.info("Launching process '{}' in lane '{}'", processKey, laneId);

        ProcessInstance instance = runtimeService.startProcessInstanceByKey(processKey, variables);

        // Track state
        runningByLane.put(laneId, instance.getId());

        processStates.put(instance.getId(), ProcessState.builder()
                .processInstanceId(instance.getId())
                .laneId(laneId)
                .build());

        logger.info("Started process '{}' with instance ID: {}", processKey, instance.getId());

        return buildProcessInstanceInfo(instance);
    }

    // ========== Queue Management ==========

    private void enqueueForLane(String processKey, Map<String, Object> variables, String laneId) {
        queuedByLane.computeIfAbsent(laneId, k -> new ConcurrentLinkedQueue<>())
                .add(PendingProcess.builder()
                        .processKey(processKey)
                        .variables(new HashMap<>(variables))
                        .laneId(laneId)
                        .queuedAt(LocalDateTime.now())
                        .build());
    }

    // ========== Status Checks ==========

    public boolean isLaneAvailable(String laneId) {
        if (laneId == null || laneId.isBlank()) {
            return true;
        }

        String processId = runningByLane.get(laneId);
        if (processId == null) {
            return true;
        }

        // Verify process is still running
        return !isProcessRunning(processId);
    }

    private boolean isProcessRunning(String processInstanceId) {
        return runtimeService.createProcessInstanceQuery()
                .processInstanceId(processInstanceId)
                .active()
                .count() > 0;
    }

    public int getTotalQueuedCount() {
        int count = 0;
        for (Queue<PendingProcess> q : queuedByLane.values()) {
            count += q.size();
        }
        return count;
    }

    public int getQueuedCountForLane(String laneId) {
        Queue<PendingProcess> queue = queuedByLane.get(laneId);
        return queue != null ? queue.size() : 0;
    }

    public int getRunningCount() {
        return processStates.size();
    }

    public List<String> getBusyLanes() {
        return runningByLane.entrySet().stream()
                .filter(e -> isProcessRunning(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    // ========== Event-Driven Process Completion ==========

    /**
     * Called by ProcessCompletionListener when a process completes or is cancelled.
     * 
     * @param processInstanceId the completed process instance ID
     */
    public synchronized void onProcessCompleted(String processInstanceId) {
        logger.info("Process completed notification received: {}", processInstanceId);

        ProcessState state = processStates.remove(processInstanceId);
        if (state == null) {
            logger.debug("No tracked state for process {}, may have been started directly", processInstanceId);
            return;
        }

        // Release lane
        if (state.laneId != null) {
            runningByLane.remove(state.laneId);
            logger.debug("Released lane '{}' for process {}", state.laneId, processInstanceId);

            // Try to run next queued process for this lane
            runNextForLane(state.laneId);
        }
    }

    private void runNextForLane(String laneId) {
        Queue<PendingProcess> queue = queuedByLane.get(laneId);
        if (queue != null && !queue.isEmpty() && isLaneAvailable(laneId)) {
            PendingProcess pending = queue.poll();
            if (pending != null) {
                logger.info("Launching next queued process for lane '{}'", laneId);
                launchProcess(pending.processKey, pending.variables, pending.laneId);
            }
        }
    }

    // ========== Fallback Cleanup (for edge cases) ==========

    /**
     * Fallback periodic cleanup for edge cases where events might be missed.
     */
    @Scheduled(fixedRate = 60000)
    public synchronized void cleanupOrphanedProcesses() {
        logger.debug("Running fallback cleanup for orphaned processes");

        List<String> toRemove = new ArrayList<>();
        for (ProcessState state : processStates.values()) {
            if (!isProcessRunning(state.processInstanceId)) {
                toRemove.add(state.processInstanceId);
                logger.warn("Found orphaned completed process: {}", state.processInstanceId);
            }
        }

        for (String processInstanceId : toRemove) {
            onProcessCompleted(processInstanceId);
        }
    }

    // ========== Direct Process Methods ==========

    /**
     * Starts a process directly without queuing (use submitProcess for queuing).
     */
    public ProcessInstanceInfo startProcess(String processKey, Map<String, Object> variables) {
        logger.info("Starting process '{}' directly (bypass queue)", processKey);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(processKey, variables);
        logger.info("Started process '{}' with instance ID: {}", processKey, instance.getId());
        return buildProcessInstanceInfo(instance);
    }

    public ProcessInstanceInfo getProcessInstance(String processInstanceId) {
        ProcessInstance instance = runtimeService.createProcessInstanceQuery()
                .processInstanceId(processInstanceId)
                .singleResult();

        if (instance != null) {
            return buildProcessInstanceInfo(instance);
        }

        HistoricProcessInstance historicInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceId(processInstanceId)
                .singleResult();

        if (historicInstance != null) {
            return buildHistoricProcessInstanceInfo(historicInstance);
        }

        return null;
    }

    public List<ProcessInstanceInfo> getRunningProcesses() {
        return runtimeService.createProcessInstanceQuery()
                .active()
                .list()
                .stream()
                .map(this::buildProcessInstanceInfo)
                .collect(Collectors.toList());
    }

    public Map<String, Object> getProcessVariables(String processInstanceId) {
        return runtimeService.getVariables(processInstanceId);
    }

    public List<WorkflowDefinitionInfo> getAvailableWorkflows() {
        return repositoryService.createProcessDefinitionQuery()
                .latestVersion()
                .list()
                .stream()
                .map(this::buildWorkflowDefinitionInfo)
                .sorted((w1, w2) -> {
                    // Sort by displayOrder (nulls last), then by workflowKey
                    if (w1.getDisplayOrder() == null && w2.getDisplayOrder() == null) {
                        return w1.getWorkflowKey().compareTo(w2.getWorkflowKey());
                    }
                    if (w1.getDisplayOrder() == null)
                        return 1;
                    if (w2.getDisplayOrder() == null)
                        return -1;
                    int orderCompare = w1.getDisplayOrder().compareTo(w2.getDisplayOrder());
                    if (orderCompare != 0)
                        return orderCompare;
                    return w1.getWorkflowKey().compareTo(w2.getWorkflowKey());
                })
                .collect(Collectors.toList());
    }

    public void terminateProcess(String processInstanceId, String reason) {
        logger.info("Terminating process {} with reason: {}", processInstanceId, reason);

        Runnable callback = terminationCallbacks.get(processInstanceId);
        if (callback != null) {
            logger.info("Calling termination callback for process {}", processInstanceId);
            try {
                callback.run();
                logger.info("Termination signal sent to process {} - worker will stop gracefully", processInstanceId);
            } catch (Exception e) {
                logger.warn("Error executing termination callback: {}", e.getMessage());
            }
        } else {
            logger.warn("No termination callback found for process {} - process may not be running", processInstanceId);
        }
    }

    public void subscribeToTermination(String processInstanceId, Runnable onTerminate) {
        terminationCallbacks.put(processInstanceId, onTerminate);
        logger.debug("Registered termination callback for process {}", processInstanceId);
    }

    public void unsubscribeFromTermination(String processInstanceId) {
        terminationCallbacks.remove(processInstanceId);
        logger.debug("Unregistered termination callback for process {}", processInstanceId);
    }

    public void subscribeToStatus(String processInstanceId, Supplier<String> statusSupplier) {
        statusCallbacks.put(processInstanceId, statusSupplier);
        logger.debug("Registered status callback for process {}", processInstanceId);
    }

    public void unsubscribeFromStatus(String processInstanceId) {
        statusCallbacks.remove(processInstanceId);
        logger.debug("Unregistered status callback for process {}", processInstanceId);
    }

    public String getWorkerStatus(String processInstanceId) {
        Supplier<String> supplier = statusCallbacks.get(processInstanceId);
        if (supplier != null) {
            try {
                return supplier.get();
            } catch (Exception e) {
                logger.warn("Error getting worker status for process {}: {}", processInstanceId, e.getMessage());
            }
        }
        return null;
    }

    public void suspendProcess(String processInstanceId) {
        logger.info("Suspending process {}", processInstanceId);
        runtimeService.suspendProcessInstanceById(processInstanceId);
    }

    public void resumeProcess(String processInstanceId) {
        logger.info("Resuming process {}", processInstanceId);
        runtimeService.activateProcessInstanceById(processInstanceId);
    }

    // ========== Helper Classes ==========

    @Data
    @Builder
    private static class PendingProcess {
        private String processKey;
        private Map<String, Object> variables;
        private String laneId;
        private LocalDateTime queuedAt;
    }

    @Data
    @Builder
    private static class ProcessState {
        private String processInstanceId;
        private String laneId;
    }

    @Data
    @Builder
    private static class ScheduledTask {
        private String scheduleId;
        private String processKey;
        private String cronExpression;
        private String laneId;
        private Map<String, Object> variables;
        private boolean enabled;
        private LocalDateTime createdAt;
        private ScheduledFuture<?> future;
    }

    // ========== Scheduling Methods ==========

    /**
     * Schedule a process to run according to a cron expression.
     * 
     * @param scheduleId     unique identifier for this schedule
     * @param processKey     the process definition key to execute
     * @param cronExpression cron expression (e.g., "0 0 2 * * ?")
     * @param laneId         lane ID for queuing when triggered
     * @param variables      variables to pass to the process
     * @return info about the created schedule
     * @throws IllegalArgumentException if scheduleId already exists or cron is
     *                                  invalid
     */
    public ScheduledProcessInfo scheduleProcess(String scheduleId, String processKey,
            String cronExpression, String laneId, Map<String, Object> variables) {

        if (scheduledTasks.containsKey(scheduleId)) {
            throw new IllegalArgumentException("Schedule already exists: " + scheduleId);
        }

        // Ensure laneId is in variables
        Map<String, Object> varsWithLane = new HashMap<>(variables);
        varsWithLane.put("laneId", laneId);

        CronTrigger trigger = new CronTrigger(cronExpression);

        ScheduledFuture<?> future = taskScheduler.schedule(
                () -> {
                    try {
                        logger.info("Scheduled trigger fired for schedule '{}'", scheduleId);
                        submitProcess(processKey, new HashMap<>(varsWithLane));
                    } catch (Exception e) {
                        logger.error("Error executing scheduled process '{}': {}", scheduleId, e.getMessage(), e);
                    }
                },
                trigger);

        ScheduledTask task = ScheduledTask.builder()
                .scheduleId(scheduleId)
                .processKey(processKey)
                .cronExpression(cronExpression)
                .laneId(laneId)
                .variables(varsWithLane)
                .enabled(true)
                .createdAt(LocalDateTime.now())
                .future(future)
                .build();

        scheduledTasks.put(scheduleId, task);

        logger.info("Created schedule '{}' for process '{}' with cron '{}'", scheduleId, processKey, cronExpression);

        return buildScheduledProcessInfo(task);
    }

    /**
     * Cancel and remove a scheduled process.
     * 
     * @param scheduleId the schedule to cancel
     * @throws IllegalArgumentException if schedule not found
     */
    public void cancelSchedule(String scheduleId) {
        ScheduledTask task = scheduledTasks.remove(scheduleId);
        if (task == null) {
            throw new IllegalArgumentException("Schedule not found: " + scheduleId);
        }

        if (task.future != null) {
            task.future.cancel(false);
        }

        logger.info("Cancelled schedule '{}'", scheduleId);
    }

    /**
     * Enable or disable a scheduled process.
     * 
     * @param scheduleId the schedule to update
     * @param enabled    true to enable, false to disable
     * @throws IllegalArgumentException if schedule not found
     */
    public void setScheduleEnabled(String scheduleId, boolean enabled) {
        ScheduledTask task = scheduledTasks.get(scheduleId);
        if (task == null) {
            throw new IllegalArgumentException("Schedule not found: " + scheduleId);
        }

        if (task.enabled == enabled) {
            return; // No change needed
        }

        if (enabled) {
            // Re-enable: create new future
            CronTrigger trigger = new CronTrigger(task.cronExpression);
            ScheduledFuture<?> future = taskScheduler.schedule(
                    () -> {
                        try {
                            logger.info("Scheduled trigger fired for schedule '{}'", scheduleId);
                            submitProcess(task.processKey, new HashMap<>(task.variables));
                        } catch (Exception e) {
                            logger.error("Error executing scheduled process '{}': {}", scheduleId, e.getMessage(), e);
                        }
                    },
                    trigger);
            task.setFuture(future);
        } else {
            // Disable: cancel future
            if (task.future != null) {
                task.future.cancel(false);
                task.setFuture(null);
            }
        }

        task.setEnabled(enabled);
        logger.info("Schedule '{}' {}", scheduleId, enabled ? "enabled" : "disabled");
    }

    /**
     * List all scheduled processes.
     * 
     * @return list of all schedules
     */
    public List<ScheduledProcessInfo> listSchedules() {
        return scheduledTasks.values().stream()
                .map(this::buildScheduledProcessInfo)
                .collect(Collectors.toList());
    }

    /**
     * Get information about a specific schedule.
     * 
     * @param scheduleId the schedule ID
     * @return schedule info, or empty if not found
     */
    public Optional<ScheduledProcessInfo> getSchedule(String scheduleId) {
        ScheduledTask task = scheduledTasks.get(scheduleId);
        return task != null ? Optional.of(buildScheduledProcessInfo(task)) : Optional.empty();
    }

    /**
     * Trigger a scheduled process immediately, ignoring its cron schedule.
     * 
     * @param scheduleId the schedule to trigger
     * @return info about the started process
     * @throws IllegalArgumentException if schedule not found
     */
    public ProcessInstanceInfo triggerScheduleNow(String scheduleId) {
        ScheduledTask task = scheduledTasks.get(scheduleId);
        if (task == null) {
            throw new IllegalArgumentException("Schedule not found: " + scheduleId);
        }

        logger.info("Manually triggering schedule '{}'", scheduleId);
        return submitProcess(task.processKey, new HashMap<>(task.variables));
    }

    private ScheduledProcessInfo buildScheduledProcessInfo(ScheduledTask task) {
        LocalDateTime nextExecution = null;
        if (task.enabled && task.future != null) {
            try {
                long delayMs = task.future.getDelay(java.util.concurrent.TimeUnit.MILLISECONDS);
                if (delayMs > 0) {
                    nextExecution = LocalDateTime.now().plusNanos(delayMs * 1_000_000);
                }
            } catch (Exception e) {
                // Future may be cancelled
            }
        }

        return ScheduledProcessInfo.builder()
                .scheduleId(task.scheduleId)
                .processKey(task.processKey)
                .cronExpression(task.cronExpression)
                .laneId(task.laneId)
                .variables(task.variables)
                .enabled(task.enabled)
                .createdAt(task.createdAt)
                .nextExecutionTime(nextExecution)
                .build();
    }

    // ========== Builder Methods ==========

    private ProcessInstanceInfo buildProcessInstanceInfo(ProcessInstance instance) {
        Map<String, Object> variables = runtimeService.getVariables(instance.getId());

        String currentActivityId = null;
        List<Execution> executions = runtimeService.createExecutionQuery()
                .processInstanceId(instance.getId())
                .list();

        if (!executions.isEmpty()) {
            currentActivityId = executions.get(0).getActivityId();
        }

        return ProcessInstanceInfo.builder()
                .processInstanceId(instance.getId())
                .processDefinitionKey(instance.getProcessDefinitionKey())
                .processDefinitionName(instance.getProcessDefinitionName())
                .variables(variables)
                .currentActivityId(currentActivityId)
                .startTime(convertToLocalDateTime(instance.getStartTime()))
                .completed(false)
                .suspended(instance.isSuspended())
                .build();
    }

    private ProcessInstanceInfo buildHistoricProcessInstanceInfo(HistoricProcessInstance instance) {
        return ProcessInstanceInfo.builder()
                .processInstanceId(instance.getId())
                .processDefinitionKey(instance.getProcessDefinitionKey())
                .processDefinitionName(instance.getProcessDefinitionName())
                .startTime(convertToLocalDateTime(instance.getStartTime()))
                .endTime(convertToLocalDateTime(instance.getEndTime()))
                .completed(instance.getEndTime() != null)
                .suspended(false)
                .build();
    }

    private WorkflowDefinitionInfo buildWorkflowDefinitionInfo(ProcessDefinition definition) {
        logger.info(">>> Building workflow info for: {}", definition.getKey());
        // Extract display order from flowable:order attribute in BPMN
        Integer displayOrder = null;
        try {
            org.flowable.bpmn.model.BpmnModel bpmnModel = repositoryService.getBpmnModel(definition.getId());
            org.flowable.bpmn.model.Process process = bpmnModel.getMainProcess();
            if (process != null) {
                // Get the flowable:order attribute from the process element
                java.util.Map<String, java.util.List<org.flowable.bpmn.model.ExtensionAttribute>> attributes = process
                        .getAttributes();

                if (attributes != null && !attributes.isEmpty()) {
                    // Try different possible keys for the order attribute
                    // Flowable may store it as "order", "flowable:order", or with full namespace
                    for (String key : new String[] { "order", "flowable:order", "{http://flowable.org/bpmn}order" }) {
                        if (attributes.containsKey(key)) {
                            java.util.List<org.flowable.bpmn.model.ExtensionAttribute> orderAttrs = attributes.get(key);
                            if (orderAttrs != null && !orderAttrs.isEmpty()) {
                                String orderValue = orderAttrs.get(0).getValue();
                                if (orderValue != null && !orderValue.isBlank()) {
                                    displayOrder = Integer.parseInt(orderValue);
                                    logger.info("  ✓ Found display order {} using key '{}'",
                                            displayOrder, key);
                                    break;
                                }
                            }
                        }
                    }

                    // Log available keys for debugging if order was not found
                    if (displayOrder == null) {
                        logger.info("  ✗ No order found. Available keys: {}", attributes.keySet());
                    }
                }
            }
        } catch (Exception e) {
            // If order attribute is not set or invalid, leave as null
            logger.warn("Error parsing display order for workflow '{}': {}",
                    definition.getKey(), e.getMessage(), e);
        }

        WorkflowDefinitionInfo result = WorkflowDefinitionInfo.builder()
                .processDefinitionId(definition.getId())
                .workflowKey(definition.getKey())
                .displayOrder(displayOrder)
                .name(definition.getName())
                .description(definition.getDescription())
                .version(definition.getVersion())
                .build();

        logger.info("  → Returning: key={}, displayOrder={}, name={}",
                result.getWorkflowKey(), result.getDisplayOrder(), result.getName());

        return result;
    }

    private LocalDateTime convertToLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
