
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

import java.time.LocalDateTime;
import java.util.List;

import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.SnapshotIndexStatus;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.core.worker.IPaginator;

/**
 * Service interface for metadata record storage and snapshot management.
 * <p>
 * Provides operations for creating, managing, and querying harvesting snapshots
 * and their associated OAI records.
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface IMetadataRecordStoreService {
	
	/**
	 * Creates a new snapshot for the given network.
	 * 
	 * @param network the network to create snapshot for
	 * @return the snapshot ID
	 */
	public Long createSnapshot(Network network);
	
	/**
	 * Saves/persists a snapshot.
	 * 
	 * @param snapshotId the snapshot ID to save
	 */
	public void saveSnapshot(Long snapshotId);
	
	/**
	 * Resets validation counters for the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @throws MetadataRecordStoreException if reset fails
	 */
	public void resetSnapshotValidationCounts(Long snapshot) throws MetadataRecordStoreException;
	
	/**
	 * Cleans data associated with the snapshot.
	 * 
	 * @param snapshotId the snapshot ID to clean
	 */
	public void cleanSnapshotData(Long snapshotId);
	
	/**
	 * Deletes a snapshot and its records.
	 * 
	 * @param snapshotId the snapshot ID to delete
	 */
	public void deleteSnapshot(Long snapshotId);
	
	/**
	 * Lists all snapshot IDs for a network.
	 * 
	 * @param networkId the network ID
	 * @param includeDeleted whether to include deleted snapshots
	 * @return list of snapshot IDs
	 */
	public List<Long> listSnapshotsIds(Long networkId, boolean includeDeleted);

	
	/**
	 * Updates the snapshot status.
	 * 
	 * @param snapshot the snapshot ID
	 * @param status the new status
	 */
	public void updateSnapshotStatus(Long snapshot, SnapshotStatus status);
	
	/**
	 * Updates the snapshot indexing status.
	 * 
	 * @param snapshot the snapshot ID
	 * @param status the new index status
	 */
	public void updateSnapshotIndexStatus(Long snapshot, SnapshotIndexStatus status);
	
	/**
	 * Updates the snapshot start timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @param datestamp the start timestamp
	 */
	public void updateSnapshotStartDatestamp(Long snapshot, LocalDateTime datestamp);
	
	/**
	 * Updates the last incremental harvest timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @param datestamp the last incremental timestamp
	 */
	public void updateSnapshotLastIncrementalDatestamp(Long snapshot, LocalDateTime datestamp);
	
	/**
	 * Updates the snapshot end timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @param datestamp the end timestamp
	 */
	public void updateSnapshotEndDatestamp(Long snapshot, LocalDateTime datestamp);
	
	/**
	 * Gets the snapshot status.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the snapshot status
	 */
	public SnapshotStatus getSnapshotStatus(Long snapshot);
	
	/**
	 * Gets the snapshot index status.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the index status
	 */
	public SnapshotIndexStatus getSnapshotIndexStatus(Long snapshot);
	
	/**
	 * Gets the snapshot start timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the start timestamp
	 */
	public LocalDateTime getSnapshotStartDatestamp(Long snapshot);
	
	/**
	 * Gets the snapshot end timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the end timestamp
	 */
	public LocalDateTime getSnapshotEndDatestamp(Long snapshot);
	
	/**
	 * Gets the last incremental harvest timestamp.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the last incremental timestamp
	 */
	public LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshot);
	
	/**
	 * Gets the number of records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record count
	 */
	public Integer getSnapshotSize(Long snapshot);


	//public void updateSnapshot(Long snapshot);
	
	/**
	 * Finds the ID of the last successful snapshot for the network.
	 * 
	 * @param network the network
	 * @return the snapshot ID, or null if none found
	 */
	public Long findLastGoodKnownSnapshot(Network network);
	
	/**
	 * Finds the ID of the last harvesting snapshot for the network.
	 * 
	 * @param network the network
	 * @return the snapshot ID, or null if none found
	 */
	public Long findLastHarvestingSnapshot(Network network);

	/**
	 * Finds a record by OAI identifier within a snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @param oaiIdentifier the OAI identifier
	 * @return the record, or null if not found
	 */
	public OAIRecord findRecordByIdentifier(Long snapshot, String oaiIdentifier);
	
	/**
	 * Finds a record by its internal record ID.
	 * 
	 * @param recordId the record ID
	 * @return the record, or null if not found
	 */
	public OAIRecord findRecordByRecordId(Long recordId);

	/**
	 * Creates a new record in the snapshot from metadata.
	 * 
	 * @param snapshot the snapshot ID
	 * @param metadata the metadata to store
	 * @return the created record
	 * @throws MetadataRecordStoreException if creation fails
	 */
	public OAIRecord createRecord(Long snapshot, OAIRecordMetadata metadata) throws MetadataRecordStoreException;
	
	/**
	 * Creates a deleted record marker in the snapshot.
	 * 
	 * @param snapshotId the snapshot ID
	 * @param identifier the OAI identifier
	 * @param dateStamp the deletion timestamp
	 * @return the created deleted record
	 * @throws MetadataRecordStoreException if creation fails
	 */
	public OAIRecord createDeletedRecord(Long snapshotId, String identifier, LocalDateTime dateStamp) throws MetadataRecordStoreException;


	/**
	 * Updates the record status and transformation flag.
	 * 
	 * @param record the record to update
	 * @param status the new status
	 * @param wasTransformed whether the record was transformed
	 * @return the updated record
	 */
	public OAIRecord updateRecordStatus(OAIRecord record, RecordStatus status, Boolean wasTransformed);

	/**
	 * Updates the original harvested metadata for a record.
	 * 
	 * @param record the record to update
	 * @param metadata the original metadata
	 * @return the updated record
	 */
	public OAIRecord updateOriginalMetadata(OAIRecord record, OAIRecordMetadata metadata);
	
	/**
	 * Retrieves the original harvested metadata for a record.
	 * 
	 * @param record the record
	 * @return the original metadata
	 * @throws OAIRecordMetadataParseException if parsing fails
	 * @throws MetadataRecordStoreException if retrieval fails
	 */
	public OAIRecordMetadata getOriginalMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException;
	
	/**
	 * Updates the published (transformed) metadata for a record.
	 * 
	 * @param record the record to update
	 * @param metadata the published metadata
	 * @return the updated record
	 */
	public OAIRecord updatePublishedMetadata(OAIRecord record, OAIRecordMetadata metadata);
	
	/**
	 * Retrieves the published (transformed) metadata for a record.
	 * 
	 * @param record the record
	 * @return the published metadata
	 * @throws OAIRecordMetadataParseException if parsing fails
	 * @throws MetadataRecordStoreException if retrieval fails
	 */
	public OAIRecordMetadata getPublishedMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException;
	
	/**
	 * Gets a paginator for untested records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 */
	public IPaginator<OAIRecord> getUntestedRecordsPaginator(Long snapshot);
	
	/**
	 * Gets a paginator for non-deleted records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 */
	public IPaginator<OAIRecord> getNotDeletedRecordsPaginator(Long snapshot);
	
	/**
	 * Gets a paginator for deleted records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 */
	public IPaginator<OAIRecord> getDeletedRecordsPaginator(Long snapshot);
	
	/**
	 * Gets a paginator for non-invalid records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 */
	public IPaginator<OAIRecord> getNotInvalidRecordsPaginator(Long snapshot);
	
	/**
	 * Gets a paginator for valid records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 */
	public IPaginator<OAIRecord> getValidRecordsPaginator(Long snapshot);
	
	/**
	 * Gets a paginator for updated records in the snapshot.
	 * 
	 * @param snapshot the snapshot ID
	 * @return the record paginator
	 * @throws MetadataRecordStoreException if retrieval fails
	 */
	public IPaginator<OAIRecord> getUpdatedRecordsPaginator(Long snapshot) throws MetadataRecordStoreException;
	
	/**
	 * Gets a paginator for record identifiers with specific status.
	 * 
	 * @param snapshot the snapshot ID
	 * @param status the record status filter
	 * @return the identifier paginator
	 */
	public IPaginator<String> getRecordIdentifiersPaginator(Long snapshot, RecordStatus status);

	/**
	 * Copies non-deleted records from a previous snapshot to the current one.
	 * 
	 * @param previousSnapshotId the source snapshot ID
	 * @param snapshotId the destination snapshot ID
	 */
	void copyNotDeletedRecordsFromSnapshot(Long previousSnapshotId, Long snapshotId);
	
	/**
	 * Gets the ID of the previous snapshot.
	 * 
	 * @param snapshotId the snapshot ID
	 * @return the previous snapshot ID, or null if none
	 */
	Long getPreviousSnapshotId(Long snapshotId);
	
	/**
	 * Sets the previous snapshot reference.
	 * 
	 * @param snapshotId the snapshot ID
	 * @param previousSnapshotId the previous snapshot ID
	 */
	void setPreviousSnapshotId(Long snapshotId, Long previousSnapshotId);
	
	/**
	 * Performs storage optimization operations.
	 */
	void optimizeStore();

}
