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

package org.lareferencia.backend.domain;

/**
 * Enumeration representing the indexing status of a network snapshot.
 * <p>
 * Tracks whether the records from a {@link NetworkSnapshot} have been successfully
 * indexed into the search engine (typically Solr).
 * </p>
 * 
 * @author LA Referencia Team
 * @see NetworkSnapshot
 */
public enum SnapshotIndexStatus {
	/** Indexing operation failed */
	FAILED,
	
	/** Records successfully indexed */
	INDEXED,
	
	/** Indexing status is unknown */
	UNKNOWN; 

	/**
	 * Converts a string representation to a SnapshotIndexStatus enum value.
	 * 
	 * @param text the string representation of the index status
	 * @return the matching SnapshotIndexStatus enum value, or null if no match found
	 */
	public static SnapshotIndexStatus fromString(String text) {
		for (SnapshotIndexStatus s : SnapshotIndexStatus.values()) {
			if (s.toString().equals(text)) {
				return s;
			}
		}
		return null;
	}
}
