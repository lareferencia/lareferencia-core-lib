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

package org.lareferencia.core.domain;

/**
 * Enumeration representing the processing status of a network snapshot.
 * <p>
 * Defines the various states a {@link NetworkSnapshot} can be in during its lifecycle,
 * from initialization through harvesting, validation, and indexing.
 * </p>
 * 
 * @author LA Referencia Team
 * @see NetworkSnapshot
 */
public enum SnapshotStatus {

	/** Snapshot has been created but harvesting has not started */
	INITIALIZED,
	
	/** Currently harvesting records from the source */
	HARVESTING,
	
	/** Retrying harvest after a temporary failure */
	RETRYING,
	
	/** Harvesting completed with errors */
	HARVESTING_FINISHED_ERROR,
	
	/** Harvesting completed successfully */
	HARVESTING_FINISHED_VALID,
	
	/** Harvesting was manually stopped */
	HARVESTING_STOPPED,
	
	/** Currently indexing records into the search engine */
	INDEXING,
	
	/** Indexing completed with errors */
	INDEXING_FINISHED_ERROR,
	
	/** Indexing completed successfully */
	INDEXING_FINISHED_VALID,
	
	/** Snapshot is valid and ready for use */
	VALID,
	
	/** Status is unknown or undefined */
	UNKNOWN,
	
	/** Incremental harvest found no new records */
	EMPTY_INCREMENTAL;
	
	
	/**
	 * Converts a string representation to a SnapshotStatus enum value.
	 * 
	 * @param text the string representation of the status
	 * @return the matching SnapshotStatus enum value, or null if no match found
	 */
	 public static SnapshotStatus fromString(String text) {
	    for (SnapshotStatus s : SnapshotStatus.values()) {
	      if ( s.toString().equals(text)  ) {
	        return s;
	      }
	    }
	    return null;
	  }
}
