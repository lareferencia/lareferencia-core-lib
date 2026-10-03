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
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.lareferencia.core.worker.IWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Legacy implementation of INetworkActionExecutor using TaskManager.
 * <p>
 * This implementation preserves the original NetworkActionManager behavior,
 * using in-memory queues managed by TaskManager for worker execution.
 * </p>
 * <p>
 * Activated when: {@code workflow.engine=legacy} (default)
 * </p>
 *
 * @author LA Referencia Team
 */
public class LegacyNetworkActionExecutor implements INetworkActionExecutor {

    private static final Logger logger = LogManager.getLogger(LegacyNetworkActionExecutor.class);

    private final TaskManager taskManager;
    private final ApplicationContext applicationContext;
    private final NetworkRepository networkRepository;

    private final ApplicationActionCatalogService actionCatalog;
    private final NetworkActionConfigurationService networkActionConfiguration;
    private final WorkerConfigurationApplier workerConfigurationApplier;

    private volatile List<NetworkAction> actions;
    private volatile Map<String, NetworkAction> actionsByName;

    public LegacyNetworkActionExecutor(TaskManager taskManager,
            ApplicationContext applicationContext,
            NetworkRepository networkRepository,
            ApplicationActionCatalogService actionCatalog,
            NetworkActionConfigurationService networkActionConfiguration,
            WorkerConfigurationApplier workerConfigurationApplier) {
        this.taskManager = taskManager;
        this.applicationContext = applicationContext;
        this.networkRepository = networkRepository;
        this.actionCatalog = actionCatalog;
        this.networkActionConfiguration = networkActionConfiguration;
        this.workerConfigurationApplier = workerConfigurationApplier;
        this.actions = new ArrayList<>();
        this.actionsByName = new HashMap<>();
    }

    @Override
    public String getEngineType() {
        return "legacy";
    }

    /**
     * Sets the available actions and builds the lookup map.
     *
     * @param actions list of network actions
     */
    @Override
    public void setActions(List<NetworkAction> actions) {
        Map<String, NetworkAction> byName = new HashMap<>();
        for (NetworkAction action : actions) byName.put(action.getName(), action);
        this.actions = List.copyOf(actions);
        this.actionsByName = Map.copyOf(byName);
    }

    /**
     * Gets the list of available actions from bean configuration.
     *
     * @return list of network actions
     */
    @Override
    public List<NetworkAction> getAvailableActions() {
        // Legacy actions remain discovered from the same Spring beans/XML as
        // before. Bean name order is only the deterministic bootstrap order;
        // after reconciliation the installation catalogue is authoritative.
        List<NetworkAction> discovered = applicationContext.getBeansOfType(NetworkAction.class).entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue).toList();
        if (!discovered.isEmpty()) setActions(discovered);
        return actionCatalog.order(getEngineType(), actions);
    }

    /**
     * Gets all configuration properties from all actions.
     *
     * @return list of network properties
     */
    @Override
    public List<NetworkProperty> getProperties() {
        List<NetworkProperty> properties = new ArrayList<>();
        for (NetworkAction action : actions) {
            properties.addAll(action.getProperties());
        }
        return properties;
    }

    @Override
    public void executeAction(String actionName, boolean isIncremental, Network network) {
        submitAction(actionName, isIncremental, network);
    }

    @Override
    public TaskSubmission submitAction(String actionName, boolean isIncremental, Network network) {
        actionCatalog.requireEnabled(getEngineType(), actionName);
        NetworkAction action = actionsByName.get(actionName);
        if (action == null) throw new ApplicationActionPolicyException("ACTION_NOT_FOUND", "Action is not configured: " + actionName);
        return taskManager.submitWorkers(prepareAction(action, isIncremental, network)).requireAccepted();
    }

    @Override
    public void executeAllActions(Network network) { submitAllActions(network); }

    @Override
    public TaskSubmission submitAllActions(Network network) {
        return taskManager.submitWorkers(prepareAllActions(network)).requireAccepted();
    }

    private List<IWorker<?>> prepareAllActions(Network network) {
        List<IWorker<?>> workers = new ArrayList<>();
        for (NetworkAction action : getAvailableActions()) {
            if (actionCatalog.isEffectivelyEnabled(getEngineType(), action.getName())
                    && networkActionConfiguration.canSchedule(network, getEngineType(), action)) {
                workers.addAll(prepareAction(action, action.isIncremental(), network));
            }
        }
        return workers;
    }

    @SuppressWarnings("unchecked")
    private List<IWorker<?>> prepareAction(NetworkAction action, boolean incremental, Network network) {
        List<IWorker<?>> workers = new ArrayList<>();
        for (String beanName : action.getWorkers()) {
            IWorker<NetworkRunningContext> worker = (IWorker<NetworkRunningContext>) applicationContext.getBean(beanName);
            workerConfigurationApplier.apply(worker, getEngineType(), action, beanName);
            worker.setIncremental(incremental);
            worker.setRunningContext(actionContext(network, action));
            workers.add(worker);
        }
        return workers;
    }

    private NetworkRunningContext actionContext(Network network, NetworkAction action) {
        return new NetworkRunningContext(network, action.getName(),
                networkActionConfiguration.effectiveConfiguration(network, getEngineType(), action.getName()));
    }

    @Override
    public void killAndUnqueueActions(Network network) {
        String contextId = NetworkRunningContext.buildID(network);
        taskManager.cancelAllByRunningContextID(contextId);
    }

    @Override
    public void scheduleNetwork(Network network) {
        String contextId = NetworkRunningContext.buildID(network);
        String cron = network.getScheduleCronExpression();
        if (cron == null || cron.isBlank()) {
            taskManager.clearScheduleByRunningContextID(contextId);
            return;
        }
        Long networkId = network.getId();
        taskManager.scheduleWorkers(contextId, "AllActions", () -> {
            Network current = networkRepository.findById(networkId)
                    .orElseThrow(() -> new IllegalStateException("Scheduled network no longer exists: " + networkId));
            return prepareAllActions(current);
        }, cron);
    }

    @Override
    public void rescheduleNetwork(Network network) { scheduleNetwork(network); }

    @Override
    public void scheduleAllNetworks() {
        Collection<Network> networks = networkRepository.findAll();
        for (Network network : networks) {
            scheduleNetwork(network);
        }
    }

    @Override
    public List<RunningProcessInfo> listRunning() {
        List<RunningProcessInfo> result = new ArrayList<>();

        for (IWorker<?> worker : taskManager.getAllRunningWorkers()) {
            var execution = taskManager.getWorkerSnapshot(worker).orElseThrow();
            if (execution.state() == TaskManager.ExecutionState.COMPLETED
                    || execution.state() == TaskManager.ExecutionState.FAILED
                    || execution.state() == TaskManager.ExecutionState.CANCELLED) continue;
            result.add(RunningProcessInfo.builder()
                    .processId(execution.executionId())
                    .networkAcronym(execution.networkAcronym())
                    .actionType(execution.workerName())
                    .status(execution.state() + " - " + worker.getStatus())
                    .startTime(execution.startedAt() == null ? null
                            : java.time.LocalDateTime.ofInstant(execution.startedAt(), java.time.ZoneOffset.UTC))
                    .incremental(worker.isIncremental())
                    .variables(Map.of("contextId", execution.contextId(), "groupId", execution.groupId(),
                            "executionState", execution.state().name()))
                    .engineType("legacy").build());
        }

        return result;
    }

    @Override
    public String getProcessStatus(String processId) {
        var execution = taskManager.getExecutionSnapshot(processId);
        if (execution.isPresent()) return execution.get().state().name();
        List<String> running = taskManager.getRunningTasksByRunningContextID(processId);
        if (!running.isEmpty()) {
            return String.join(", ", running);
        }
        return null;
    }

    @Override
    public void terminateProcess(String processId, String reason) {
        if (!taskManager.cancelExecution(processId)) taskManager.killAllTaskByRunningContextID(processId);
        logger.info("Terminated all tasks for context: {} reason: {}", processId, reason);
    }

    public List<TaskManager.ExecutionSnapshot> getExecutionSnapshots() { return taskManager.getExecutionSnapshots(); }

    @Override
    public int getQueuedCount() {
        return taskManager.getQueuedCount();
    }

    @Override
    public int getRunningCount() {
        return taskManager.getRunningCount();
    }

    @Override
    public List<String> getRunningTasksByContext(String runningContextID) {
        return taskManager.getRunningTasksByRunningContextID(runningContextID);
    }

    @Override
    public List<String> getQueuedTasksByContext(String runningContextID) {
        return taskManager.getQueuedTasksByRunningContextID(runningContextID);
    }

    @Override
    public List<String> getScheduledTasksByContext(String runningContextID) {
        return taskManager.getScheduledTasksByRunningContextID(runningContextID);
    }

}
