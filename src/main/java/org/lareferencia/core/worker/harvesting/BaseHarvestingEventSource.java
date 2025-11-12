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

import java.util.LinkedList;
import java.util.List;

/**
 * Base implementation for harvesting event sources.
 * <p>
 * Manages event listener registration and provides methods
 * to fire harvesting events to all registered listeners.
 * </p>
 * 
 * @author LA Referencia Team
 * @see IHarvestingEventSource
 */
public abstract class BaseHarvestingEventSource implements IHarvestingEventSource {

	/**
	 * List of registered event listeners.
	 */
	protected List<IHarvestingEventListener> listeners;

	/**
	 * Creates a new event source with an empty listener list.
	 */
	public BaseHarvestingEventSource() {
		listeners = new LinkedList<IHarvestingEventListener>();
	}

	@Override
	public void addEventListener(IHarvestingEventListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeEventListener(IHarvestingEventListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Fires a harvesting event to all registered listeners.
	 * 
	 * @param event the event to fire
	 */
	public void fireHarvestingEvent(HarvestingEvent event) {
		for (IHarvestingEventListener listener : listeners) {
			listener.harvestingEventOccurred(event);
		}
	}

}
