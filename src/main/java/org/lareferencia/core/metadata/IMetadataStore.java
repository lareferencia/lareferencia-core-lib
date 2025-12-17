
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

import java.util.function.Consumer;

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
	String storeAndReturnHash(SnapshotMetadata snapshotMetadata, String metadata);
	
	/**
	 * Retrieves metadata by its hash.
	 * 
	 * @param hash the metadata hash
	 * @return the metadata string
	 * @throws MetadataRecordStoreException if retrieval fails
	 */
	String getMetadata(SnapshotMetadata snapshotMetadata, String hash) throws MetadataRecordStoreException;

	/**
	 * Performs cleanup and optimization on the metadata store.
	 * 
	 * @return true if optimization was successful
	 */
	Boolean cleanAndOptimizeStore();

	/**
	 * Deletes metadata by its hash.
	 * 
	 * @param snapshotMetadata context containing network information
	 * @param hash the metadata hash to delete
	 * @return true if the metadata was deleted, false if it didn't exist
	 * @throws MetadataRecordStoreException if deletion fails due to an error
	 */
	boolean deleteMetadata(SnapshotMetadata snapshotMetadata, String hash) throws MetadataRecordStoreException;

	/**
	 * Iterates over all metadata hashes in the store, applying the given consumer to each.
	 * <p>
	 * This method handles resource management internally, ensuring proper cleanup
	 * of connections, file handles, and other resources after iteration completes.
	 * </p>
	 * 
	 * @param snapshotMetadata context containing network information
	 * @param hashConsumer consumer to apply to each hash
	 * @throws MetadataRecordStoreException if iteration fails
	 */
	void forEachHash(SnapshotMetadata snapshotMetadata, Consumer<String> hashConsumer) throws MetadataRecordStoreException;

}
