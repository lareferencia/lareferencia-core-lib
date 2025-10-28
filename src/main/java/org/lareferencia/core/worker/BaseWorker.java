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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lombok.Getter;
import lombok.Setter;

/**
 * Base implementation for all workers.
 * <p>
 * Provides common functionality including lifecycle management, scheduling,
 * and context handling for background processing tasks.
 * </p>
 * 
 * @param <C> the running context type
 * @author LA Referencia Team
 * @see IWorker
 */
public abstract class BaseWorker<C extends IRunningContext> implements IWorker<C> {
	
    private static Logger logger = LogManager.getLogger(BaseWorker.class);
    
	@Getter
	@Setter
	ScheduledFuture<?> scheduledFuture;
	
	/**
	 * The context containing state and configuration for this worker's execution.
	 */
	@Getter
	@Setter
	protected
	C runningContext;
	
	/**
	 * Identifier for serializing worker execution within a lane.
	 */
	@Getter
	@Setter 
	protected Long serialLaneId = -1L;
	
	/**
	 * Indicates whether this worker operates in incremental mode.
	 */
	protected boolean incremental = false;

	/**
	 * Human-readable name for this worker instance.
	 */
    @Getter
    @Setter
	protected String name = "BaseWorker";
	
	/**
	 * Creates a worker with default settings.
	 */
	public BaseWorker() {
		this.serialLaneId = -1L;
		this.name = this.getClass().getSimpleName();
	}
	
	/**
	 * Creates a worker with the specified context.
	 * 
	 * @param context the running context
	 */
	public BaseWorker(C context) {
		this.runningContext = context;
		this.name = this.getClass().getSimpleName();
	}
	
	@Override
	public void stop() {
		
		if ( scheduledFuture != null )
			scheduledFuture.cancel(true);
		logger.info("WORKER: "+ getName() +" :: stopped");
	}

	@Override
	public boolean isIncremental() {
		return incremental;
	}

	@Override
	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}
	


	

}
