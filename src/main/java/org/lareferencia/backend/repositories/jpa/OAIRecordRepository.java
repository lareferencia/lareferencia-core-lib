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

package org.lareferencia.backend.repositories.jpa;

import java.time.LocalDateTime;

import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.RecordStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;



/**
 * JPA Repository for {@link OAIRecord} entity.
 * <p>
 * Provides CRUD operations and custom query methods for managing harvested OAI records.
 * This repository includes optimized queries for batch processing and status filtering.
 * <p>
 * Key operations include:
 * </p>
 * <ul>
 *   <li>Retrieving records by snapshot and identifier</li>
 *   <li>Filtering records by status (VALID, INVALID, UNTESTED, etc.)</li>
 *   <li>Paginated queries for batch processing</li>
 *   <li>Bulk status updates and deletions</li>
 *   <li>Date-based filtering for incremental operations</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see OAIRecord
 * @see NetworkSnapshot
 * @see RecordStatus
 */
@RepositoryRestResource(path = "record", collectionResourceRel = "record")
public interface OAIRecordRepository extends JpaRepository<OAIRecord, Long> {

	/* obtener registro por snapshot_id e identifier */

	/**
	 * Finds a single record by snapshot ID and identifier.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param identifier the record identifier
	 * @return the record matching the criteria, or null if not found
	 */
	@Query("select rc from OAIRecord rc where rc.snapshot.id = ?1 and rc.identifier = ?2")
	OAIRecord findOneBySnapshotIdAndIdentifier(Long snapshotID, String identifier);


	/// Paginados y optimizados 
	
//	@Query("select rc from OAIRecord rc where rc.snapshot.id = ?1 and rc.id > ?4 and ((false=?3 AND rc.status=?2) OR (true=?3 AND rc.status<>?2)) order by rc.id asc")
//	Page<OAIRecord> findBySnapshotIdAndStatusOptimizedOrderByRecordID(Long snapshotID, RecordStatus status, Boolean negateStatus, Long lastRecordID, Pageable pageable);

//	@Query("select rc from OAIRecord rc where rc.snapshot.id = ?1 and rc.datestamp > ?4 and rc.id > ?5 and ((false=?3 AND rc.status=?2) OR (true=?3 AND rc.status<>?2)) order by rc.id asc")
//	Page<OAIRecord> findBySnapshotIdAndStatusAndDateOptimizedOrderByRecordID(Long snapshotID, RecordStatus status, Boolean negateStatus, LocalDateTime date, Long lastRecordID, Pageable pageable);

//	@Query("select rc from OAIRecord rc where rc.snapshot.id = ?1 and rc.id > ?2 order by rc.id asc")
//	Page<OAIRecord> findBySnapshotIdOptimizedOrderByRecordID(Long snapshotID, Long lastRecordID, Pageable pageable);
	
//	@Query("select rc from OAIRecord rc where rc.snapshot.id = ?1 and rc.datestamp >= ?2 and rc.id > ?3 order by rc.id asc")
//	Page<OAIRecord> findBySnapshotIdAndDateOptimizedOrderByRecordID(Long snapshotID, LocalDateTime from, Long lastRecordID, Pageable pageable);

	/**
	 * Finds records by snapshot ID and status, with cursor-based pagination.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param status the record status to filter by
	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
	 * @param pageable pagination parameters
	 * @return page of records matching the criteria
	 */
	// find by snapshot_id and status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, Long lastRecordId, Pageable pageable);

	/**
	 * Finds records by snapshot ID excluding a specific status, with cursor-based pagination.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param status the record status to exclude
	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
	 * @param pageable pagination parameters
	 * @return page of records not matching the status
	 */
	// find by snapshot_id and not status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusNotAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, Long lastRecordId, Pageable pageable);

 	/**
 	 * Finds records by snapshot ID and datestamp range, with cursor-based pagination.
 	 * 
 	 * @param snapshotID the snapshot ID
 	 * @param from the minimum datestamp to include
 	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
 	 * @param pageable pagination parameters
 	 * @return page of records with datestamp >= from
 	 */
 	// find by snapshot_id and status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, LocalDateTime from, Long lastRecordId, Pageable pageable);

	/**
	 * Finds records by snapshot ID, status, and datestamp range, with cursor-based pagination.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param status the record status to filter by
	 * @param from the minimum datestamp to include
	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
	 * @param pageable pagination parameters
	 * @return page of records matching all criteria
	 */
	// find by snapshot_id and status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, LocalDateTime from, Long lastRecordId, Pageable pageable);

	/**
	 * Finds records by snapshot ID excluding a status and filtered by datestamp, with cursor-based pagination.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param status the record status to exclude
	 * @param from the minimum datestamp to include
	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
	 * @param pageable pagination parameters
	 * @return page of records not matching the status with datestamp >= from
	 */
	// find by snapshot_id and not status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusNotAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, LocalDateTime from, Long lastRecordId, Pageable pageable);

	/**
	 * Finds all records by snapshot ID with cursor-based pagination.
	 * 
	 * @param snapshotID the snapshot ID
	 * @param lastRecordId the last record ID from previous page (for cursor pagination)
	 * @param pageable pagination parameters
	 * @return page of all records in the snapshot
	 */
	// find by snapshot_id and status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndIdGreaterThanOrderByIdAsc(Long snapshotID, Long lastRecordId, Pageable pageable);

	/**
	 * Deletes all records for a specific snapshot.
	 * 
	 * @param snapshot_id the snapshot ID whose records should be deleted
	 */
	@Modifying
	@Transactional
	@Query("delete from OAIRecord r where r.snapshot.id = ?1")
	void deleteBySnapshotID(Long snapshot_id);

	/**
	 * Lists record identifiers by snapshot and status with pagination.
	 * 
	 * @param snapshotId the snapshot ID
	 * @param status the record status to filter by
	 * @param pageable pagination parameters
	 * @return page of record identifiers matching the criteria
	 */
	@Query("select rc.identifier from OAIRecord rc where rc.snapshot.id = ?1 and rc.status = ?2")
	Page<String> listIdentifiersBySnapshotAndStatus(Long snapshotId, RecordStatus status, Pageable pageable);

	/**
	 * Counts records by snapshot and status array.
	 * 
	 * @param snapshot the snapshot entity
	 * @param status array of statuses to count
	 * @return total count of records matching any of the statuses
	 */
	Long countBySnapshotAndStatusIn(NetworkSnapshot snapshot, RecordStatus[] status);

	/**
	 * Counts records by snapshot and transformed flag.
	 * 
	 * @param snapshot the snapshot entity
	 * @param b whether to count transformed (true) or non-transformed (false) records
	 * @return total count of records matching the transformed flag
	 */
	Long countBySnapshotAndTransformed(NetworkSnapshot snapshot, boolean b);
}
