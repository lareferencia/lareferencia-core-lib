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
import org.springframework.beans.factory.annotation.Autowired;

import jakarta.annotation.PostConstruct;

import java.util.List;

/**
 * Manager for network actions that delegates to a configurable executor.
 * <p>
 * This class serves as a facade that maintains backward compatibility while
 * allowing the underlying execution engine to be switched via configuration.
 * </p>
 * <p>
 * The execution engine is selected via the {@code workflow.engine} property:
 * <ul>
 * <li>{@code workflow.engine=legacy} (default) - Uses TaskManager</li>
 * <li>{@code workflow.engine=flowable} - Uses WorkflowService with BPMN</li>
 * </ul>
 * </p>
 *
 * @author LA Referencia Team
 */
public class NetworkActionkManager {

	@Autowired
	private INetworkActionExecutor executor;

	/**
	 * Constructs a new network action manager.
	 */
	public NetworkActionkManager() {
		// Empty constructor - actions come from executor
	}

	/**
	 * Initializes the executor and schedules all networks.
	 */
	@PostConstruct
	public void initialize() {
		// Schedule all networks
		scheduleAllNetworks();
	}

	/**
	 * Gets the list of available actions from the executor.
	 * For legacy: reads from bean configuration.
	 * For Flowable: reads from BPMN process definitions.
	 *
	 * @return list of available network actions
	 */
	public List<NetworkAction> getActions() {
		return executor.getAvailableActions();
	}

	/**
	 * Gets all configuration properties from available actions.
	 *
	 * @return list of network properties from all actions
	 */
	public List<NetworkProperty> getProperties() {
		return executor.getProperties();
	}

	/**
	 * Executes workers associated with each action for the given network.
	 *
	 * @param network the network to execute actions for
	 */
	public void executeActions(Network network) {
		executor.executeAllActions(network);
	}

	/**
	 * Executes a specific action by name for the given network.
	 *
	 * @param actionName    the name of the action to execute
	 * @param isIncremental whether to run in incremental mode
	 * @param network       the network to execute the action for
	 */
	public synchronized void executeAction(String actionName, boolean isIncremental, Network network) {
		executor.executeAction(actionName, isIncremental, network);
	}

	/**
	 * Kills and unqueues all actions for a network.
	 *
	 * @param network the network to clean up
	 */
	public void killAndUnqueueActions(Network network) {
		executor.killAndUnqueueActions(network);
	}

	/**
	 * Sets the list of available actions.
	 * Delegates to the executor - only effective for legacy mode.
	 *
	 * @param actions list of network actions
	 */
	public void setActions(List<NetworkAction> actions) {
		executor.setActions(actions);
	}

	/**
	 * Schedules harvest actions for all networks.
	 */
	public void scheduleAllNetworks() {
		executor.scheduleAllNetworks();
	}

	/**
	 * Reschedules harvest actions for a specific network.
	 *
	 * @param network the network to reschedule
	 */
	public void rescheduleNetwork(Network network) {
		executor.rescheduleNetwork(network);
	}

	/**
	 * Lists all currently running processes.
	 *
	 * @return list of running process information
	 */
	public List<RunningProcessInfo> listRunning() {
		return executor.listRunning();
	}

	/**
	 * Gets the status of a specific process.
	 *
	 * @param processId the process identifier
	 * @return status description
	 */
	public String getProcessStatus(String processId) {
		return executor.getProcessStatus(processId);
	}

	/**
	 * Terminates a running process.
	 *
	 * @param processId the process to terminate
	 * @param reason    the reason for termination
	 */
	public void terminateProcess(String processId, String reason) {
		executor.terminateProcess(processId, reason);
	}

	/**
	 * Gets the total count of queued processes.
	 *
	 * @return number of queued processes
	 */
	public int getQueuedCount() {
		return executor.getQueuedCount();
	}

	/**
	 * Gets the count of currently running processes.
	 *
	 * @return number of running processes
	 */
	public int getRunningCount() {
		return executor.getRunningCount();
	}

	/**
	 * Gets the underlying executor for direct access if needed.
	 *
	 * @return the network action executor
	 */
	public INetworkActionExecutor getExecutor() {
		return executor;
	}

	// ========== Legacy Compatibility Methods ==========
	// These methods provide backward compatibility with controllers that used
	// TaskManager directly

	/**
	 * Gets running tasks for a specific running context.
	 * For legacy compatibility with controllers.
	 *
	 * @param runningContextID the context identifier
	 * @return list of running task descriptions
	 */
	public List<String> getRunningTasksByRunningContextID(String runningContextID) {
		return executor.getRunningTasksByContext(runningContextID);
	}

	/**
	 * Gets queued tasks for a specific running context.
	 * For legacy compatibility with controllers.
	 *
	 * @param runningContextID the context identifier
	 * @return list of queued task descriptions
	 */
	public List<String> getQueuedTasksByRunningContextID(String runningContextID) {
		return executor.getQueuedTasksByContext(runningContextID);
	}

	/**
	 * Gets scheduled tasks for a specific running context.
	 * For legacy compatibility with controllers.
	 *
	 * @param runningContextID the context identifier
	 * @return list of scheduled task descriptions
	 */
	public List<String> getScheduledTasksByRunningContextID(String runningContextID) {
		return executor.getScheduledTasksByContext(runningContextID);
	}
}
