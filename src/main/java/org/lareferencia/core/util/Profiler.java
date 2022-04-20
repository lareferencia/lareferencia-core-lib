
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

package org.lareferencia.core.util;


import org.apache.logging.log4j.Logger;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Profiler {
	
	private Boolean profileMode = true;
	private long startTime = 0L;
	private long profileTime = 0L;
	private StringBuffer buffer;
	
	public Profiler(Boolean profileMode, String msg) {
		super();
		this.profileMode = profileMode;
		this.buffer = new StringBuffer(msg);
	}
	
	public Profiler start() {
		if (profileMode)	
			profileTime = startTime = System.nanoTime();
		return this;
	}
	
	public void messure(String msg) {
		messure(msg, false);
	}

	public void messure(String msg, boolean newLine) {
		
		if (profileMode) {	
			
			buffer.append( (newLine ? "\n":"") + msg + ": ");
			buffer.append( (System.nanoTime()-profileTime)/1000000 );
			buffer.append(" ms | ");
			
			profileTime = System.nanoTime();
		}	
		
	}
	
	public void report(Logger logger) {
		
		if (profileMode) {	
			
			buffer.append(  "\nTotal time: ");
			buffer.append( (System.nanoTime()-startTime)/1000000 );
			buffer.append(" ms \n");
		
			logger.info( buffer.toString() );
			
		}
		
	}
	
	public String report() {
		
		if (profileMode) {	
			
			buffer.append(  "\nTotal time: ");
			buffer.append( (System.nanoTime()-startTime)/1000000 );
			buffer.append(" ms \n");
		
			return buffer.toString() ;
			
		} else
			return "";
		
	}

}
