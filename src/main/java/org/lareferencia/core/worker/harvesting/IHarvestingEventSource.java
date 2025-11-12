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

package org.lareferencia.core.worker.harvesting;

/**
 * Interface for sources that generate harvesting events.
 * <p>
 * Implements the observer pattern for harvesting operations,
 * allowing listeners to be notified of harvesting progress and status.
 * </p>
 * 
 * @author LA Referencia Team
 * @see IHarvestingEventListener
 * @see HarvestingEvent
 */
public interface IHarvestingEventSource {
	
	/**
	 * Registers a listener to receive harvesting events.
	 * 
	 * @param listener the listener to add
	 */
	public void addEventListener(IHarvestingEventListener listener);

	/**
	 * Removes a previously registered listener.
	 * 
	 * @param listener the listener to remove
	 */
	public void removeEventListener(IHarvestingEventListener listener);
}
