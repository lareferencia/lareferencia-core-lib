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



public abstract class BaseWorker<C extends IRunningContext> implements IWorker<C> {
	
    private static Logger logger = LogManager.getLogger(BaseWorker.class);
    
	@Getter
	@Setter
	ScheduledFuture<?> scheduledFuture;
	
	@Getter
	@Setter
	protected
	C runningContext;
	
	@Getter
	@Setter 
	protected Long serialLaneId = -1L;
	
	protected boolean incremental = false;

    @Getter
    @Setter
	protected String name = "BaseWorker";
	
	public BaseWorker() {
		this.serialLaneId = -1L;
		this.name = this.getClass().getSimpleName();
	}
	
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
