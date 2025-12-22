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

package org.lareferencia.core.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.flowable.WorkflowService;
import org.lareferencia.core.flowable.dto.ProcessInstanceInfo;
import org.lareferencia.core.repository.jpa.NetworkRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flowable-based implementation of INetworkActionExecutor using
 * WorkflowService.
 * <p>
 * This implementation uses Flowable BPMN processes for workflow orchestration,
 * providing process persistence, recovery, and advanced workflow capabilities.
 * </p>
 * <p>
 * Activated when: {@code workflow.engine=flowable}
 * </p>
 *
 * @author LA Referencia Team
 */
public class FlowableNetworkActionExecutor implements INetworkActionExecutor {

    private static final Logger logger = LogManager.getLogger(FlowableNetworkActionExecutor.class);

    /** Default process key when executing all actions */
    private static final String DEFAULT_ALL_ACTIONS_PROCESS = "networkProcessing";

    private final WorkflowService workflowService;
    private final NetworkRepository networkRepository;

    public FlowableNetworkActionExecutor(WorkflowService workflowService,
            NetworkRepository networkRepository) {
        this.workflowService = workflowService;
        this.networkRepository = networkRepository;
    }

    @Override
    public void executeAction(String actionName, boolean isIncremental, Network network) {
        logger.info("Executing action '{}' for network '{}' via Flowable",
                actionName, network.getAcronym());

        // Use actionName directly as processKey (e.g., "harvesting" -> process
        // "harvesting")
        String processKey = actionName;

        Map<String, Object> variables = buildProcessVariables(network, isIncremental);
        variables.put("actionName", actionName);

        try {
            ProcessInstanceInfo instance = workflowService.submitProcess(processKey, variables);
            logger.info("Submitted process '{}' for network '{}', instance: {}",
                    processKey, network.getAcronym(),
                    instance.getProcessInstanceId() != null ? instance.getProcessInstanceId() : "queued");
        } catch (Exception e) {
            logger.error("Failed to submit process '{}' for network '{}': {}",
                    processKey, network.getAcronym(), e.getMessage(), e);
        }
    }

    @Override
    public void executeAllActions(Network network) {
        // In Flowable mode, "all actions" means running the main harvesting process
        // which includes all steps defined in the BPMN
        logger.info("Executing all actions for network '{}' via Flowable", network.getAcronym());

        Map<String, Object> variables = buildProcessVariables(network, false);

        try {
            ProcessInstanceInfo instance = workflowService.submitProcess(DEFAULT_ALL_ACTIONS_PROCESS, variables);
            logger.info("Submitted harvesting process for network '{}', instance: {}",
                    network.getAcronym(),
                    instance.getProcessInstanceId() != null ? instance.getProcessInstanceId() : "queued");
        } catch (Exception e) {
            logger.error("Failed to submit process for network '{}': {}",
                    network.getAcronym(), e.getMessage(), e);
        }
    }

    @Override
    public void killAndUnqueueActions(Network network) {
        String laneId = buildLaneId(network);
        logger.info("Killing and unqueuing actions for network '{}' (lane: {})",
                network.getAcronym(), laneId);

        // Find and terminate running processes for this network
        for (ProcessInstanceInfo process : workflowService.getRunningProcesses()) {
            Object processLaneId = process.getVariables().get("laneId");
            if (laneId.equals(processLaneId)) {
                workflowService.terminateProcess(process.getProcessInstanceId(),
                        "Killed by killAndUnqueueActions");
            }
        }
    }

    @Override
    public void scheduleNetwork(Network network) {
        String cronExpression = network.getScheduleCronExpression();
        if (cronExpression == null || cronExpression.isEmpty()) {
            logger.debug("No cron expression for network '{}', skipping schedule", network.getAcronym());
            return;
        }

        String scheduleId = buildScheduleId(network);
        String laneId = buildLaneId(network);

        Map<String, Object> variables = buildProcessVariables(network, false);

        try {
            workflowService.scheduleProcess(scheduleId, DEFAULT_ALL_ACTIONS_PROCESS,
                    cronExpression, laneId, variables);
            logger.info("Scheduled network '{}' with cron '{}' (scheduleId: {})",
                    network.getAcronym(), cronExpression, scheduleId);
        } catch (IllegalArgumentException e) {
            logger.warn("Schedule already exists for network '{}', skipping", network.getAcronym());
        } catch (Exception e) {
            logger.error("Failed to schedule network '{}': {}", network.getAcronym(), e.getMessage(), e);
        }
    }

    @Override
    public void rescheduleNetwork(Network network) {
        String scheduleId = buildScheduleId(network);

        // Cancel existing schedule
        try {
            workflowService.cancelSchedule(scheduleId);
            logger.debug("Cancelled existing schedule for network '{}'", network.getAcronym());
        } catch (IllegalArgumentException e) {
            // No existing schedule, that's fine
        }

        // Create new schedule
        scheduleNetwork(network);
    }

    @Override
    public void scheduleAllNetworks() {
        Collection<Network> networks = networkRepository.findAll();
        logger.info("Scheduling {} networks via Flowable", networks.size());

        for (Network network : networks) {
            scheduleNetwork(network);
        }
    }

    @Override
    public List<RunningProcessInfo> listRunning() {
        List<RunningProcessInfo> result = new ArrayList<>();

        for (ProcessInstanceInfo process : workflowService.getRunningProcesses()) {
            Map<String, Object> vars = process.getVariables();

            result.add(RunningProcessInfo.builder()
                    .processId(process.getProcessInstanceId())
                    .networkAcronym((String) vars.get("networkAcronym"))
                    .actionType(process.getProcessDefinitionKey())
                    .status(workflowService.getWorkerStatus(process.getProcessInstanceId()))
                    .startTime(process.getStartTime())
                    .incremental((Boolean) vars.get("incremental"))
                    .variables(vars)
                    .engineType("flowable")
                    .build());
        }

        return result;
    }

    @Override
    public String getProcessStatus(String processId) {
        return workflowService.getWorkerStatus(processId);
    }

    @Override
    public void terminateProcess(String processId, String reason) {
        workflowService.terminateProcess(processId, reason);
    }

    @Override
    public int getQueuedCount() {
        return workflowService.getTotalQueuedCount();
    }

    @Override
    public int getRunningCount() {
        return workflowService.getRunningCount();
    }

    /**
     * Gets available actions from Flowable BPMN process definitions.
     * Each process definition is exposed as an action.
     *
     * @return list of actions derived from BPMN processes
     */
    @Override
    public List<NetworkAction> getAvailableActions() {
        List<NetworkAction> actions = new ArrayList<>();

        // Get process definitions from WorkflowService and convert to NetworkAction
        for (var processInfo : workflowService.getAvailableProcesses()) {
            NetworkAction action = new NetworkAction();
            action.setName(processInfo.getProcessKey());
            action.setDescription(processInfo.getName() != null ? processInfo.getName() : processInfo.getProcessKey());
            action.setRunOnSchedule(true);
            action.setIncremental(false);
            actions.add(action);
        }

        return actions;
    }

    /**
     * In Flowable mode, actions come from BPMN definitions, not beans.
     * This method is a no-op for Flowable executor.
     *
     * @param actions ignored in Flowable mode
     */
    @Override
    public void setActions(List<NetworkAction> actions) {
        // In Flowable mode, actions are derived from BPMN process definitions
        // Bean-configured actions are ignored
        logger.debug(
                "setActions() called on FlowableNetworkActionExecutor - actions come from BPMN definitions, ignoring bean config");
    }

    /**
     * In Flowable mode, returns empty list as properties are not used.
     *
     * @return empty list
     */
    @Override
    public List<NetworkProperty> getProperties() {
        // Flowable mode doesn't use NetworkProperty configuration
        return new ArrayList<>();
    }

    @Override
    public List<String> getRunningTasksByContext(String runningContextID) {
        List<String> result = new ArrayList<>();
        for (ProcessInstanceInfo process : workflowService.getRunningProcesses()) {
            Object laneId = process.getVariables().get("laneId");
            if (runningContextID.equals(laneId) ||
                    (laneId != null && runningContextID.contains(laneId.toString()))) {
                result.add(process.getProcessDefinitionKey() + " - " +
                        workflowService.getWorkerStatus(process.getProcessInstanceId()));
            }
        }
        return result;
    }

    @Override
    public List<String> getQueuedTasksByContext(String runningContextID) {
        // TODO: Implement queued tasks query from WorkflowService
        return new ArrayList<>();
    }

    @Override
    public List<String> getScheduledTasksByContext(String runningContextID) {
        // TODO: Implement scheduled tasks query from WorkflowService
        return new ArrayList<>();
    }

    // ========== Helper Methods ==========

    private Map<String, Object> buildProcessVariables(Network network, boolean incremental) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("networkId", network.getId());
        variables.put("networkAcronym", network.getAcronym());
        variables.put("laneId", buildLaneId(network));
        variables.put("incremental", incremental);
        return variables;
    }

    private String buildLaneId(Network network) {
        return "network-" + network.getId();
    }

    private String buildScheduleId(Network network) {
        return "schedule-network-" + network.getId();
    }
}
