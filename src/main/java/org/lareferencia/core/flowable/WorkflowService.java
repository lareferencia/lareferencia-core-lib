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
import org.lareferencia.core.flowable.dto.ProcessDefinitionInfo;
import org.lareferencia.core.flowable.dto.ProcessInstanceInfo;
import org.lareferencia.core.flowable.exception.QueueFullException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Service for managing Flowable BPMN process instances with concurrency
 * control.
 * <p>
 * Provides lane-based queuing:
 * <ul>
 * <li>Network queue (lane null/-1): One process per network</li>
 * <li>Global serial lane (lane 0): Processes run one at a time globally</li>
 * <li>Serial lanes (lane > 0): Processes in same lane run serially</li>
 * </ul>
 * </p>
 * 
 * @author LA Referencia Team
 */
@Service
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

    /** Tracks running process by network: networkId -> processInstanceId */
    private final Map<Long, String> runningByNetwork = new ConcurrentHashMap<>();

    /** Queued processes by network */
    private final Map<Long, Queue<PendingProcess>> queuedByNetwork = new ConcurrentHashMap<>();

    /** Tracks running process by lane: laneId -> processInstanceId */
    private final Map<Long, String> runningByLane = new ConcurrentHashMap<>();

    /** Queued processes by lane */
    private final Map<Long, Queue<PendingProcess>> queuedByLane = new ConcurrentHashMap<>();

    /** Track process states for cleanup */
    private final Map<String, ProcessState> processStates = new ConcurrentHashMap<>();

    /** Callbacks for worker termination - called when process is terminated */
    private final Map<String, Runnable> terminationCallbacks = new ConcurrentHashMap<>();

    /** Callbacks for worker status - queried to get current worker status */
    private final Map<String, Supplier<String>> statusCallbacks = new ConcurrentHashMap<>();

    // ========== Initialization ==========

    /**
     * Cleanup interrupted processes and pending jobs on startup.
     * This prevents automatic recovery of processes that were running when the
     * system crashed.
     */
    @PostConstruct
    public void cleanupOnStartup() {
        logger.info("WorkflowService initializing - cleaning up interrupted processes and pending jobs");

        try {
            // Delete all pending async jobs to prevent recovery
            long deletedJobs = managementService.createJobQuery()
                    .list()
                    .stream()
                    .peek(job -> {
                        logger.info("Deleting pending job: {} (process: {}, retries: {})",
                                job.getId(), job.getProcessInstanceId(), job.getRetries());
                        managementService.deleteJob(job.getId());
                    })
                    .count();

            logger.info("Deleted {} pending async jobs", deletedJobs);

            // Delete all timer jobs
            long deletedTimers = managementService.createTimerJobQuery()
                    .list()
                    .stream()
                    .peek(timer -> {
                        logger.info("Deleting pending timer: {} (process: {})",
                                timer.getId(), timer.getProcessInstanceId());
                        managementService.deleteTimerJob(timer.getId());
                    })
                    .count();

            logger.info("Deleted {} pending timer jobs", deletedTimers);

            // Delete all deadletter jobs (failed jobs)
            long deletedDeadLetters = managementService.createDeadLetterJobQuery()
                    .list()
                    .stream()
                    .peek(deadLetter -> {
                        logger.info("Deleting dead letter job: {} (process: {})",
                                deadLetter.getId(), deadLetter.getProcessInstanceId());
                        managementService.deleteDeadLetterJob(deadLetter.getId());
                    })
                    .count();

            logger.info("Deleted {} dead letter jobs", deletedDeadLetters);

            // Delete all active process instances (interrupted processes)
            long deletedProcesses = runtimeService.createProcessInstanceQuery()
                    .list()
                    .stream()
                    .peek(process -> {
                        logger.info("Deleting interrupted process: {} (definition: {})",
                                process.getId(), process.getProcessDefinitionKey());
                        runtimeService.deleteProcessInstance(process.getId(),
                                "Cleanup on startup - process was interrupted by system shutdown");
                    })
                    .count();

            logger.info("Deleted {} interrupted process instances", deletedProcesses);

            logger.info("Startup cleanup completed - system ready for new processes");

        } catch (Exception e) {
            logger.error("Error during startup cleanup: {}", e.getMessage(), e);
            // Don't throw - allow service to start even if cleanup fails
        }
    }

    /**
     * Graceful shutdown - terminate all running processes and clear queues.
     * This ensures the application can exit cleanly without waiting for Flowable.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("WorkflowService shutting down - terminating all running processes");

        // Clear all queues first to prevent new processes from starting
        queuedByNetwork.clear();
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
        runningByNetwork.clear();
        runningByLane.clear();
        processStates.clear();

        logger.info("WorkflowService shutdown completed - terminated {} processes", terminatedCount);
    }

    // ========== Submit Process (with queuing) ==========

    /**
     * Submits a process for execution. If the network or lane is busy,
     * the process is queued.
     * 
     * @param processKey the process definition key
     * @param variables  the process variables (must include networkId)
     * @return info about the started or queued process
     * @throws QueueFullException if the queue is at maximum capacity
     */
    public synchronized ProcessInstanceInfo submitProcess(String processKey, Map<String, Object> variables) {
        Long networkId = (Long) variables.get("networkId");
        Long laneId = config.getLaneForProcess(processKey);

        logger.info("Submitting process '{}' for network {} with lane {}", processKey, networkId, laneId);

        if (laneId == null || laneId < 0) {
            // Network queue: one process per network
            return submitToNetworkQueue(processKey, variables, networkId);
        } else {
            // Serial lane (0 = global, >0 = specific)
            return submitToLane(processKey, variables, networkId, laneId);
        }
    }

    private ProcessInstanceInfo submitToNetworkQueue(String processKey, Map<String, Object> variables, Long networkId) {
        if (!isNetworkBusy(networkId)) {
            return launchProcess(processKey, variables, networkId, null);
        } else {
            // Queue for network
            if (getTotalQueuedCount() >= config.getMaxQueuedProcesses()) {
                throw new QueueFullException("Max queued processes reached: " + config.getMaxQueuedProcesses());
            }

            enqueueForNetwork(processKey, variables, networkId);
            logger.info("Process '{}' queued for network {}", processKey, networkId);

            return ProcessInstanceInfo.builder()
                    .processDefinitionKey(processKey)
                    .variables(variables)
                    .completed(false)
                    .suspended(false)
                    .build();
        }
    }

    private ProcessInstanceInfo submitToLane(String processKey, Map<String, Object> variables,
            Long networkId, Long laneId) {
        // Check both: network not busy AND lane available
        boolean canLaunch = !isNetworkBusy(networkId) && isLaneAvailable(laneId);

        if (canLaunch) {
            return launchProcess(processKey, variables, networkId, laneId);
        } else {
            // Queue for lane
            int maxQueuedForLane = config.getMaxQueuedForLane(laneId);
            if (getQueuedCountForLane(laneId) >= maxQueuedForLane) {
                throw new QueueFullException(
                        "Max queued processes for lane " + laneId + " reached: " + maxQueuedForLane);
            }

            enqueueForLane(processKey, variables, networkId, laneId);
            logger.info("Process '{}' queued for lane {}", processKey, laneId);

            return ProcessInstanceInfo.builder()
                    .processDefinitionKey(processKey)
                    .variables(variables)
                    .completed(false)
                    .suspended(false)
                    .build();
        }
    }

    private ProcessInstanceInfo launchProcess(String processKey, Map<String, Object> variables,
            Long networkId, Long laneId) {
        logger.info("Launching process '{}' for network {} in lane {}", processKey, networkId, laneId);

        ProcessInstance instance = runtimeService.startProcessInstanceByKey(processKey, variables);

        // Track state
        if (networkId != null) {
            runningByNetwork.put(networkId, instance.getId());
        }
        if (laneId != null && laneId >= 0) {
            runningByLane.put(laneId, instance.getId());
        }

        processStates.put(instance.getId(), ProcessState.builder()
                .processInstanceId(instance.getId())
                .networkId(networkId)
                .laneId(laneId)
                .build());

        logger.info("Started process '{}' with instance ID: {}", processKey, instance.getId());

        return buildProcessInstanceInfo(instance);
    }

    // ========== Queue Management ==========

    private void enqueueForNetwork(String processKey, Map<String, Object> variables, Long networkId) {
        queuedByNetwork.computeIfAbsent(networkId, k -> new ConcurrentLinkedQueue<>())
                .add(PendingProcess.builder()
                        .processKey(processKey)
                        .variables(new HashMap<>(variables))
                        .networkId(networkId)
                        .laneId(null)
                        .queuedAt(LocalDateTime.now())
                        .build());
    }

    private void enqueueForLane(String processKey, Map<String, Object> variables,
            Long networkId, Long laneId) {
        queuedByLane.computeIfAbsent(laneId, k -> new ConcurrentLinkedQueue<>())
                .add(PendingProcess.builder()
                        .processKey(processKey)
                        .variables(new HashMap<>(variables))
                        .networkId(networkId)
                        .laneId(laneId)
                        .queuedAt(LocalDateTime.now())
                        .build());
    }

    // ========== Status Checks ==========

    public boolean isNetworkBusy(Long networkId) {
        if (networkId == null)
            return false;

        String processId = runningByNetwork.get(networkId);
        if (processId == null)
            return false;

        // Verify process is still running
        return isProcessRunning(processId);
    }

    public boolean isLaneAvailable(Long laneId) {
        if (laneId == null || laneId < 0)
            return true;

        String processId = runningByLane.get(laneId);
        if (processId == null)
            return true;

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
        for (Queue<PendingProcess> q : queuedByNetwork.values()) {
            count += q.size();
        }
        for (Queue<PendingProcess> q : queuedByLane.values()) {
            count += q.size();
        }
        return count;
    }

    public int getQueuedCountForLane(Long laneId) {
        Queue<PendingProcess> queue = queuedByLane.get(laneId);
        return queue != null ? queue.size() : 0;
    }

    public int getRunningCount() {
        return processStates.size();
    }

    public List<Long> getBusyNetworks() {
        return runningByNetwork.entrySet().stream()
                .filter(e -> isProcessRunning(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    // ========== Event-Driven Process Completion ==========

    /**
     * Called by ProcessCompletionListener when a process completes or is cancelled.
     * This is the primary mechanism for releasing resources - replaces polling.
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

        // Release network
        if (state.networkId != null) {
            runningByNetwork.remove(state.networkId);
            logger.debug("Released network {} for process {}", state.networkId, processInstanceId);

            // Try to run next queued process for this network
            runNextForNetwork(state.networkId);
        }

        // Release lane
        if (state.laneId != null && state.laneId >= 0) {
            runningByLane.remove(state.laneId);
            logger.debug("Released lane {} for process {}", state.laneId, processInstanceId);

            // Try to run next queued process for this lane
            runNextForLane(state.laneId);
        }
    }

    private void runNextForNetwork(Long networkId) {
        Queue<PendingProcess> queue = queuedByNetwork.get(networkId);
        if (queue != null && !queue.isEmpty() && !isNetworkBusy(networkId)) {
            PendingProcess pending = queue.poll();
            if (pending != null) {
                logger.info("Launching next queued process for network {}", networkId);
                launchProcess(pending.processKey, pending.variables,
                        pending.networkId, pending.laneId);
            }
        }
    }

    private void runNextForLane(Long laneId) {
        Queue<PendingProcess> queue = queuedByLane.get(laneId);
        if (queue != null && !queue.isEmpty() && isLaneAvailable(laneId)) {
            PendingProcess pending = queue.poll();
            if (pending != null && !isNetworkBusy(pending.networkId)) {
                logger.info("Launching next queued process for lane {}", laneId);
                launchProcess(pending.processKey, pending.variables,
                        pending.networkId, pending.laneId);
            } else if (pending != null) {
                // Put back if network is busy
                queue.add(pending);
            }
        }
    }

    // ========== Fallback Cleanup (for edge cases) ==========

    /**
     * Fallback periodic cleanup for edge cases where events might be missed.
     * The primary mechanism is event-driven via onProcessCompleted().
     */
    @Scheduled(fixedRate = 60000) // Every 60 seconds (reduced from 10s)
    public synchronized void cleanupOrphanedProcesses() {
        logger.debug("Running fallback cleanup for orphaned processes");

        // Clean finished processes that weren't caught by events
        List<String> toRemove = new ArrayList<>();
        for (ProcessState state : processStates.values()) {
            if (!isProcessRunning(state.processInstanceId)) {
                toRemove.add(state.processInstanceId);
                logger.warn("Found orphaned completed process: {}", state.processInstanceId);
            }
        }

        // Process any orphaned completions
        for (String processInstanceId : toRemove) {
            onProcessCompleted(processInstanceId);
        }
    }

    // ========== Original Methods ==========

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

    public List<ProcessDefinitionInfo> getAvailableProcesses() {
        return repositoryService.createProcessDefinitionQuery()
                .latestVersion()
                .list()
                .stream()
                .map(this::buildProcessDefinitionInfo)
                .collect(Collectors.toList());
    }

    public void terminateProcess(String processInstanceId, String reason) {
        logger.info("Terminating process {} with reason: {}", processInstanceId, reason);

        // Notify the worker to stop via callback
        // The worker will detect the stop signal and terminate gracefully,
        // which allows the process to complete normally without needing
        // deleteProcessInstance
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

    /**
     * Subscribe to termination events for a process.
     * The callback will be called when terminateProcess() is invoked.
     * 
     * @param processInstanceId the process instance ID
     * @param onTerminate       callback to run on termination (typically
     *                          worker.stop())
     */
    public void subscribeToTermination(String processInstanceId, Runnable onTerminate) {
        terminationCallbacks.put(processInstanceId, onTerminate);
        logger.debug("Registered termination callback for process {}", processInstanceId);
    }

    /**
     * Unsubscribe from termination events for a process.
     * Call this when process/worker completes normally.
     * 
     * @param processInstanceId the process instance ID
     */
    public void unsubscribeFromTermination(String processInstanceId) {
        terminationCallbacks.remove(processInstanceId);
        logger.debug("Unregistered termination callback for process {}", processInstanceId);
    }

    /**
     * Subscribe to status updates for a process.
     * The supplier will be called when getWorkerStatus() is invoked.
     * 
     * @param processInstanceId the process instance ID
     * @param statusSupplier    supplier that returns the current worker status
     */
    public void subscribeToStatus(String processInstanceId, Supplier<String> statusSupplier) {
        statusCallbacks.put(processInstanceId, statusSupplier);
        logger.debug("Registered status callback for process {}", processInstanceId);
    }

    /**
     * Unsubscribe from status updates for a process.
     * 
     * @param processInstanceId the process instance ID
     */
    public void unsubscribeFromStatus(String processInstanceId) {
        statusCallbacks.remove(processInstanceId);
        logger.debug("Unregistered status callback for process {}", processInstanceId);
    }

    /**
     * Gets the current status of a running worker.
     * 
     * @param processInstanceId the process instance ID
     * @return the worker status, or null if not found
     */
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
        private Long networkId;
        private Long laneId;
        private LocalDateTime queuedAt;
    }

    @Data
    @Builder
    private static class ProcessState {
        private String processInstanceId;
        private Long networkId;
        private Long laneId;
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

    private ProcessDefinitionInfo buildProcessDefinitionInfo(ProcessDefinition definition) {
        return ProcessDefinitionInfo.builder()
                .processDefinitionId(definition.getId())
                .processKey(definition.getKey())
                .name(definition.getName())
                .description(definition.getDescription())
                .version(definition.getVersion())
                .build();
    }

    private LocalDateTime convertToLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
