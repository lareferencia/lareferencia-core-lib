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


public class NetworkAction {

	public NetworkAction() {
		workers = new ArrayList<>();
		properties = new ArrayList<>();
	}
	
	@Getter
	@Setter
	List<String> workers;
	
	private boolean incremental = false;
	
	@Getter
	@Setter
	List<NetworkProperty> properties;
	
	@Getter
	@Setter
	Boolean runOnSchedule = false; 
	
	@Getter
	@Setter
	String name = "DUMMY";
	
	@Getter
	@Setter
	String description = "DUMMY";

	public boolean isIncremental() {
		return incremental;
	}

	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

}
