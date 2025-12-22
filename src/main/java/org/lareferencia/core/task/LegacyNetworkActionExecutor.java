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
import org.lareferencia.core.worker.BaseWorker;
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

    private List<NetworkAction> actions;
    private Map<String, NetworkAction> actionsByName;

    public LegacyNetworkActionExecutor(TaskManager taskManager,
            ApplicationContext applicationContext,
            NetworkRepository networkRepository) {
        this.taskManager = taskManager;
        this.applicationContext = applicationContext;
        this.networkRepository = networkRepository;
        this.actions = new ArrayList<>();
        this.actionsByName = new HashMap<>();
    }

    /**
     * Sets the available actions and builds the lookup map.
     *
     * @param actions list of network actions
     */
    @Override
    public void setActions(List<NetworkAction> actions) {
        this.actionsByName = new HashMap<>();
        this.actions = actions;
        for (NetworkAction action : actions) {
            actionsByName.put(action.getName(), action);
        }
    }

    /**
     * Gets the list of available actions from bean configuration.
     *
     * @return list of network actions
     */
    @Override
    public List<NetworkAction> getAvailableActions() {
        return actions;
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
        logger.debug("Executing action: {}", actionName);

        NetworkAction action = actionsByName.get(actionName);

        if (action != null) {
            for (String workerBeanName : action.getWorkers()) {
                try {
                    logger.debug("Executing worker: {} incremental: {}", workerBeanName, isIncremental);

                    @SuppressWarnings("unchecked")
                    IWorker<NetworkRunningContext> worker = (IWorker<NetworkRunningContext>) applicationContext
                            .getBean(workerBeanName);
                    worker.setIncremental(isIncremental);
                    worker.setRunningContext(new NetworkRunningContext(network));
                    taskManager.launchWorker(worker);

                } catch (Exception e) {
                    logger.error("Issues found creating worker: {} using action: {} on network: {}",
                            workerBeanName, action.getName(), network.getAcronym());
                    e.printStackTrace();
                }
            }
        } else {
            logger.error("The action: {} doesn't exist!", actionName);
        }
    }

    @Override
    public void executeAllActions(Network network) {
        for (NetworkAction action : actions) {
            // Check if any property is true
            boolean anyPropertyIsTrue = false;
            for (NetworkProperty property : action.getProperties()) {
                anyPropertyIsTrue |= network.getBooleanPropertyValue(property.getName());
            }

            // Always run overrides property check
            anyPropertyIsTrue |= action.getAllwaysRunOnSchedule();

            // Run if scheduled AND some property is true (or always run is set)
            if (action.getRunOnSchedule() && anyPropertyIsTrue) {
                for (String workerBeanName : action.getWorkers()) {
                    try {
                        @SuppressWarnings("unchecked")
                        IWorker<NetworkRunningContext> worker = (IWorker<NetworkRunningContext>) applicationContext
                                .getBean(workerBeanName);
                        worker.setIncremental(action.isIncremental());
                        worker.setRunningContext(new NetworkRunningContext(network));
                        taskManager.launchWorker(worker);

                    } catch (Exception e) {
                        logger.error("Issues found creating worker: {} using action: {} on network: {}",
                                workerBeanName, action.getName(), network.getAcronym());
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void killAndUnqueueActions(Network network) {
        String contextId = NetworkRunningContext.buildID(network);
        taskManager.clearQueueByRunningContextID(contextId);
        taskManager.killAllTaskByRunningContextID(contextId);
    }

    @Override
    public void scheduleNetwork(Network network) {
        logger.info("Scheduling harvest: {} with cron: {}",
                network.getAcronym(), network.getScheduleCronExpression());

        if (network.getScheduleCronExpression() != null) {
            taskManager.scheduleWorker(new AllActionsWorker(network), network.getScheduleCronExpression());
        }
    }

    @Override
    public void rescheduleNetwork(Network network) {
        String contextId = NetworkRunningContext.buildID(network);
        taskManager.clearScheduleByRunningContextID(contextId);

        logger.info("Rescheduling harvest: {} with cron: {}",
                network.getAcronym(), network.getScheduleCronExpression());

        if (network.getScheduleCronExpression() != null && !network.getScheduleCronExpression().isEmpty()) {
            taskManager.scheduleWorker(new AllActionsWorker(network), network.getScheduleCronExpression());
        }
    }

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
            String contextId = worker.getRunningContext() != null ? worker.getRunningContext().getId() : "unknown";
            String networkAcronym = null;

            // Extract network acronym from context if available
            if (worker.getRunningContext() instanceof NetworkRunningContext) {
                Network network = ((NetworkRunningContext) worker.getRunningContext()).getNetwork();
                if (network != null) {
                    networkAcronym = network.getAcronym();
                }
            }

            result.add(RunningProcessInfo.builder()
                    .processId(contextId)
                    .networkAcronym(networkAcronym)
                    .actionType(worker.toString())
                    .status(worker.getStatus())
                    .incremental(worker.isIncremental())
                    .engineType("legacy")
                    .build());
        }

        return result;
    }

    @Override
    public String getProcessStatus(String processId) {
        // Legacy uses runningContextID as processId
        List<String> running = taskManager.getRunningTasksByRunningContextID(processId);
        if (!running.isEmpty()) {
            return String.join(", ", running);
        }
        return null;
    }

    @Override
    public void terminateProcess(String processId, String reason) {
        // In legacy, processId is the runningContextID
        taskManager.killAllTaskByRunningContextID(processId);
        logger.info("Terminated all tasks for context: {} reason: {}", processId, reason);
    }

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

    /**
     * Inner worker class that executes all actions for a network.
     */
    private class AllActionsWorker extends BaseWorker<NetworkRunningContext> {

        public AllActionsWorker(Network network) {
            super(new NetworkRunningContext(network));
        }

        @Override
        public String toString() {
            return "AllActions";
        }

        @Override
        public void run() {
            executeAllActions(runningContext.getNetwork());
        }
    }
}
