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


public interface IWorker<C extends IRunningContext> extends Runnable {

	public void run();
	public void stop();
	
	public ScheduledFuture<?> getScheduledFuture();
	public void setScheduledFuture(ScheduledFuture<?> scheduledFuture);

	public void setName(String name);
	public String getName();
	
	public void setRunningContext(C context);
	public C getRunningContext();
	
	public Long getSerialLaneId();
	public void setSerialLaneId(Long id);
	
	public boolean isIncremental();
	void setIncremental(boolean incremental);
}
