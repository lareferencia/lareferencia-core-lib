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

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents an action to be executed on a network.
 */
public class NetworkAction {

	/**
	 * Constructs a new network action with empty workers and properties lists.
	 */
	public NetworkAction() {
		workers = new ArrayList<>();
		properties = new ArrayList<>();
	}
	
	/**
	 * List of worker names to be executed as part of this action.
	 */
	@Getter
	@Setter
	List<String> workers;
	
	/**
	 * Flag indicating whether this action runs in incremental mode.
	 */
	private boolean incremental = false;
	
	/**
	 * List of properties associated with this network action.
	 */
	@Getter
	@Setter
	List<NetworkProperty> properties;
	
	/**
	 * Flag indicating whether this action should run on schedule.
	 */
	@Getter
	@Setter
	Boolean runOnSchedule = false; 
	
	/**
	 * Flag indicating whether this action should always run on schedule regardless of other conditions.
	 */
	@Getter
    @Setter
    Boolean allwaysRunOnSchedule = false; 
	
	/**
	 * The name identifier of this action.
	 */
	@Getter
	@Setter
	String name = "DUMMY";
	
	/**
	 * A human-readable description of this action's purpose.
	 */
	@Getter
	@Setter
	String description = "DUMMY";
	
	/**
	 * Checks if this action operates in incremental mode.
	 *
	 * @return true if incremental mode is enabled, false otherwise
	 */
	public boolean isIncremental() {
		return incremental;
	}

	/**
	 * Sets whether this action should operate in incremental mode.
	 *
	 * @param incremental true to enable incremental mode, false otherwise
	 */
	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

}
