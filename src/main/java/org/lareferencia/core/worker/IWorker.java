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

package org.lareferencia.core.worker;

import java.util.concurrent.ScheduledFuture;

/**
 * Base interface for all worker types.
 * <p>
 * Workers are background tasks that process data in the harvesting and
 * validation pipeline. They can be scheduled and stopped as needed.
 * </p>
 * 
 * @param <C> the running context type
 * @author LA Referencia Team
 */
public interface IWorker<C extends IRunningContext> extends Runnable {

	/**
	 * Executes the worker's main processing logic.
	 */
	public void run();
	
	/**
	 * Stops the worker execution.
	 */
	public void stop();
	
	/**
	 * Gets the scheduled future for this worker.
	 * 
	 * @return the scheduled future
	 */
	public ScheduledFuture<?> getScheduledFuture();
	
	/**
	 * Sets the scheduled future for this worker.
	 * 
	 * @param scheduledFuture the scheduled future to set
	 */
	public void setScheduledFuture(ScheduledFuture<?> scheduledFuture);

	/**
	 * Sets the name of this worker.
	 * 
	 * @param name the worker name
	 */
	public void setName(String name);
	
	/**
	 * Gets the name of this worker.
	 * 
	 * @return the worker name
	 */
	public String getName();
	
	/**
	 * Sets the running context for this worker.
	 * 
	 * @param context the running context
	 */
	public void setRunningContext(C context);
	
	/**
	 * Gets the running context for this worker.
	 * 
	 * @return the running context
	 */
	public C getRunningContext();
	
	/**
	 * Gets the serial lane identifier for this worker.
	 * 
	 * @return the serial lane ID
	 */
	public Long getSerialLaneId();
	
	/**
	 * Sets the serial lane identifier for this worker.
	 * 
	 * @param id the serial lane ID
	 */
	public void setSerialLaneId(Long id);
	
	/**
	 * Checks if this worker operates in incremental mode.
	 * 
	 * @return true if incremental, false otherwise
	 */
	public boolean isIncremental();
	
	/**
	 * Sets the incremental mode for this worker.
	 * 
	 * @param incremental true to enable incremental mode
	 */
	void setIncremental(boolean incremental);
}