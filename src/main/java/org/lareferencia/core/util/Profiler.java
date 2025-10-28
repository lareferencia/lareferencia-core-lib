
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

/**
 * Utility class for performance profiling and timing measurements.
 * Provides methods to measure execution time of code segments and generate timing reports.
 */
@Getter
@Setter
public class Profiler {
	
	/**
	 * Flag indicating whether profiling is enabled.
	 */
	private Boolean profileMode = true;
	
	/**
	 * The starting time of the profiling session in nanoseconds.
	 */
	private long startTime = 0L;
	
	/**
	 * The current checkpoint time for incremental measurements in nanoseconds.
	 */
	private long profileTime = 0L;
	
	/**
	 * Buffer for accumulating profiling messages and measurements.
	 */
	private StringBuffer buffer;
	
	/**
	 * Constructs a new Profiler with the specified profile mode and initial message.
	 * 
	 * @param profileMode true to enable profiling, false to disable
	 * @param msg the initial message for the profiling session
	 */
	public Profiler(Boolean profileMode, String msg) {
		super();
		this.profileMode = profileMode;
		this.buffer = new StringBuffer(msg);
	}
	
	/**
	 * Starts the profiling timer.
	 * 
	 * @return this Profiler instance for method chaining
	 */
	public Profiler start() {
		if (profileMode)	
			profileTime = startTime = System.nanoTime();
		return this;
	}
	
	/**
	 * Measures elapsed time since the last measurement with the given message.
	 * 
	 * @param msg the message describing this measurement point
	 */
	public void messure(String msg) {
		messure(msg, false);
	}

	/**
	 * Measures elapsed time since the last measurement with the given message.
	 * 
	 * @param msg the message describing this measurement point
	 * @param newLine true to start the message on a new line, false otherwise
	 */
	public void messure(String msg, boolean newLine) {
		
		if (profileMode) {	
			
			buffer.append( (newLine ? "\n":"") + msg + ": ");
			buffer.append( (System.nanoTime()-profileTime)/1000000 );
			buffer.append(" ms | ");
			
			profileTime = System.nanoTime();
		}	
		
	}
	
	/**
	 * Generates and logs the final profiling report with total execution time.
	 * 
	 * @param logger the logger to use for outputting the report
	 */
	public void report(Logger logger) {
		
		if (profileMode) {	
			
			buffer.append(  "\nTotal time: ");
			buffer.append( (System.nanoTime()-startTime)/1000000 );
			buffer.append(" ms \n");
		
			logger.info( buffer.toString() );
			
		}
		
	}
	
	/**
	 * Generates and returns the final profiling report with total execution time.
	 * 
	 * @return the profiling report as a string
	 */
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
