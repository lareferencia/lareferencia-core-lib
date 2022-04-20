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

package org.lareferencia.backend.taskmanager;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.repositories.jpa.NetworkRepository;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.IWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.*;


public class NetworkActionkManager {
	
	
	private static Logger logger = LogManager.getLogger(NetworkActionkManager.class);

	
	@Getter
	@Autowired
	private TaskManager taskManager;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	private NetworkRepository networkRepository;

	
	/**
	 * Esto permite que se programen inicialmente todas las redes cuando se inicializa el repository
	 */
	@Autowired
	public void setNetworkRepository(NetworkRepository repository) {
		this.networkRepository = repository;
		scheduleAllNetworks();
	}

	public NetworkActionkManager() {
		actionsByName = new HashMap<String, NetworkAction>();
		actions = new ArrayList<NetworkAction>();		
	}
	
	@Getter
	List<NetworkAction> actions;
	
	private Map<String, NetworkAction> actionsByName;
	
	
	public List<NetworkProperty> getProperties() {
		
		ArrayList<NetworkProperty> properties = new ArrayList<NetworkProperty>();
		
		for (NetworkAction action : actions ) {
			properties.addAll( action.getProperties() );
		}
		
		return properties;
	}

	
	/**
	 * Lanza los workers asociados a cada acción
	 * @param network
	 */
	public void executeActions(Network network) {
		
		// para cada propiedad mapeada
		for ( NetworkAction action: actions ) {
			
			// mira si alguna propiedad de esta accion es true, de otro modo no ejecutará
			boolean anyPropertyisTrue = false;
			for (NetworkProperty property: action.getProperties() )
				anyPropertyisTrue |= network.getBooleanPropertyValue(property.getName());
			
			// si es programada y alguna propiedad de esa red es true
			if ( action.getRunOnSchedule() && anyPropertyisTrue ) {
			
				for ( String workerBeanName : action.getWorkers() ) {
				
					try {
						IWorker<NetworkRunningContext> worker = (IWorker<NetworkRunningContext>) applicationContext.getBean( workerBeanName );
						worker.setIncremental(action.isIncremental()); // si la acción es potencialmente incremental lanza la version incremental
						worker.setRunningContext( new NetworkRunningContext(network) );
						taskManager.launchWorker(worker);
					
					} catch (Exception e) {
						logger.error( "Issues found creating worker: " + workerBeanName +" using action/property: " + action.getName() + " on the network: " + network.getAcronym() ); 
						e.printStackTrace();
					}
				
				}
			}
		}
	}
	
	/**
	 * Lanza los workers asociados a cada acción
	 * @param network
	 */
	public synchronized void executeAction(String actionName, boolean isIncremental,  Network network) {
		
		
		logger.debug("Ejecutando action: " + actionName);

		NetworkAction action = actionsByName.get(actionName);
		
		if ( action != null ) {
			
			for ( String workerBeanName : action.getWorkers() ) {
			
				try {
					logger.debug("Executing worker: " + workerBeanName + " incremental: " + action.isIncremental());
					
					IWorker<NetworkRunningContext> worker = (IWorker<NetworkRunningContext>) applicationContext.getBean( workerBeanName );
					worker.setIncremental( isIncremental );
					worker.setRunningContext( new NetworkRunningContext(network) );
					taskManager.launchWorker(worker);
				
				} catch (Exception e) {
					logger.error( "Issues found creating worker: " + workerBeanName +" using action/property: " + action.getName() + " on the network: " + network.getAcronym() ); 
					e.printStackTrace();
				}
			
			}
		} else {
			logger.error("The action: " + actionName + " doesn't exist !!");
		}
	}
	
	/**
	 * Limpia la cola de proceso y el proceos actual de una red
	 * @param network
	 */
	public void killAndUnqueueActions(Network network) {
		
		taskManager.clearQueueByRunningContextID( NetworkRunningContext.buildID(network) );
		taskManager.killAllTaskByRunningContextID( NetworkRunningContext.buildID(network) );
			
	}

	public void setActions(List<NetworkAction> actions) {
		
		actionsByName = new HashMap<String, NetworkAction>();
		this.actions = actions;
		
		for (NetworkAction action:actions) 
			actionsByName.put(action.getName(), action);
		
	}

	
	public void scheduleAllNetworks() {

		Collection<Network> networks = networkRepository.findAll();

		for (Network network : networks) {
			
			logger.info("Scheduling harvest:" + network.getAcronym() + " ::::: " + network.getScheduleCronExpression());

			if ( network.getScheduleCronExpression() != null )
				taskManager.scheduleWorker(new AllActionsWorker(network), network.getScheduleCronExpression() );
		}
	}
	
	public void rescheduleNetwork(Network network) {
		
		taskManager.clearScheduleByRunningContextID( NetworkRunningContext.buildID(network) );
		
		logger.info("Scheduling harvest:" + network.getAcronym() + " ::::: " + network.getScheduleCronExpression());

		if ( network.getScheduleCronExpression() != null && network.getScheduleCronExpression() != "" )
			taskManager.scheduleWorker(new AllActionsWorker(network), network.getScheduleCronExpression() );
		
	}
		
		
	private class AllActionsWorker extends BaseWorker<NetworkRunningContext> {
		
		@Override
		public String toString() {
			return "AllActions";
		}

		public AllActionsWorker(Network network) {
			super( new NetworkRunningContext(network) );
		}


		@Override
		public void run() {
			executeActions(runningContext.getNetwork());
		}
	}

}
