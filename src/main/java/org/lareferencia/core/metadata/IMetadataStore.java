
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

package org.lareferencia.core.metadata;

/**
 * Interface for metadata storage and retrieval.
 * <p>
 * Provides hash-based metadata storage for deduplication.
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface IMetadataStore {
	
	/**
	 * Stores metadata and returns its hash.
	 * 
	 * @param metadata the metadata string to store
	 * @return the calculated hash of the metadata
	 */
	String storeAndReturnHash(String metadata);
	
	/**
	 * Retrieves metadata by its hash.
	 * 
	 * @param hash the metadata hash
	 * @return the metadata string
	 * @throws MetadataRecordStoreException if retrieval fails
	 */
	String getMetadata(String hash) throws MetadataRecordStoreException;

	/**
	 * Performs cleanup and optimization on the metadata store.
	 * 
	 * @return true if optimization was successful
	 */
	Boolean cleanAndOptimizeStore();

}
