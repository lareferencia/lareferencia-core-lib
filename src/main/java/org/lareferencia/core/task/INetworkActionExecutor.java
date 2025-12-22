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

import org.lareferencia.core.domain.Network;

import java.util.List;

/**
 * Interface for network action execution engines.
 * <p>
 * This abstraction allows switching between different execution backends:
 * <ul>
 * <li><b>Legacy</b>: Uses TaskManager for in-memory queue management</li>
 * <li><b>Flowable</b>: Uses WorkflowService with BPMN process
 * orchestration</li>
 * </ul>
 * </p>
 * <p>
 * The implementation is selected via the property {@code workflow.engine}:
 * <ul>
 * <li>{@code workflow.engine=legacy} - Uses LegacyNetworkActionExecutor
 * (default)</li>
 * <li>{@code workflow.engine=flowable} - Uses
 * FlowableNetworkActionExecutor</li>
 * </ul>
 * </p>
 *
 * @author LA Referencia Team
 */
public interface INetworkActionExecutor {

    /**
     * Executes a specific action for a network.
     *
     * @param actionName    the name of the action to execute
     * @param isIncremental whether to run in incremental mode
     * @param network       the network to execute the action for
     */
    void executeAction(String actionName, boolean isIncremental, Network network);

    /**
     * Executes all scheduled actions for a network.
     *
     * @param network the network to execute actions for
     */
    void executeAllActions(Network network);

    /**
     * Kills running processes and clears queued actions for a network.
     *
     * @param network the network to clean up
     */
    void killAndUnqueueActions(Network network);

    /**
     * Schedules a network for automatic execution based on its cron expression.
     *
     * @param network the network to schedule
     */
    void scheduleNetwork(Network network);

    /**
     * Reschedules a network, clearing existing schedules first.
     *
     * @param network the network to reschedule
     */
    void rescheduleNetwork(Network network);

    /**
     * Schedules all networks from the repository.
     */
    void scheduleAllNetworks();

    /**
     * Lists all currently running processes/workers.
     *
     * @return list of running process information
     */
    List<RunningProcessInfo> listRunning();

    /**
     * Gets the status of a specific process.
     *
     * @param processId the process identifier
     * @return status description, or null if not found
     */
    String getProcessStatus(String processId);

    /**
     * Terminates a running process.
     *
     * @param processId the process to terminate
     * @param reason    the reason for termination
     */
    void terminateProcess(String processId, String reason);

    /**
     * Gets the total count of queued processes.
     *
     * @return number of queued processes
     */
    int getQueuedCount();

    /**
     * Gets the count of currently running processes.
     *
     * @return number of running processes
     */
    int getRunningCount();

    /**
     * Gets the list of available actions.
     * For legacy: reads from bean configuration.
     * For Flowable: reads from BPMN process definitions.
     *
     * @return list of available network actions
     */
    List<NetworkAction> getAvailableActions();

    /**
     * Sets the available actions (for legacy mode bean configuration).
     *
     * @param actions list of network actions
     */
    void setActions(List<NetworkAction> actions);

    /**
     * Gets all configuration properties from available actions.
     *
     * @return list of network properties
     */
    List<NetworkProperty> getProperties();

    // ========== Context-specific methods (for backward compatibility) ==========

    /**
     * Gets running tasks for a specific running context.
     *
     * @param runningContextID the context identifier
     * @return list of running task descriptions
     */
    List<String> getRunningTasksByContext(String runningContextID);

    /**
     * Gets queued tasks for a specific running context.
     *
     * @param runningContextID the context identifier
     * @return list of queued task descriptions
     */
    List<String> getQueuedTasksByContext(String runningContextID);

    /**
     * Gets scheduled tasks for a specific running context.
     *
     * @param runningContextID the context identifier
     * @return list of scheduled task descriptions
     */
    List<String> getScheduledTasksByContext(String runningContextID);
}
