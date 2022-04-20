
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

import org.lareferencia.backend.domain.Network;

import lombok.Getter;
import lombok.Setter;

public class NetworkRunningContext implements IRunningContext {
	
	static final String ID_PREFIX = "NETWORK::";
	
	public static String buildID(Network network) {
		return ID_PREFIX + network.getId();
	}
	
	@Getter
	@Setter
	Network network;

	public NetworkRunningContext(Network network) {
		super();
		this.network = network;
	}

	@Override
	public String getId() {
		return buildID(network); 
	}
	
	
    @Override
    public String toString() {
        return this.network.getAcronym() + "(id:" + this.network.getId() + ")";
    }	
}
