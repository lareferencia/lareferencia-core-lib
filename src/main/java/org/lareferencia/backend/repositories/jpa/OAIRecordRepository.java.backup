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



@RepositoryRestResource(path = "record", collectionResourceRel = "record")
public interface OAIRecordRepository extends JpaRepository<OAIRecord, Long> {

	/* obtener registro por snapshot_id e identifier */

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

	// find by snapshot_id and status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, Long lastRecordId, Pageable pageable);

	// find by snapshot_id and not status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusNotAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, Long lastRecordId, Pageable pageable);

 	// find by snapshot_id and status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, LocalDateTime from, Long lastRecordId, Pageable pageable);

	// find by snapshot_id and status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, LocalDateTime from, Long lastRecordId, Pageable pageable);

	// find by snapshot_id and not status and datestamp >= from and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndStatusNotAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(Long snapshotID, RecordStatus status, LocalDateTime from, Long lastRecordId, Pageable pageable);

	// find by snapshot_id and status and id > lastRecordId order by id asc
	Page<OAIRecord> findBySnapshotIdAndIdGreaterThanOrderByIdAsc(Long snapshotID, Long lastRecordId, Pageable pageable);

	@Modifying
	@Transactional
	@Query("delete from OAIRecord r where r.snapshot.id = ?1")
	void deleteBySnapshotID(Long snapshot_id);

	@Query("select rc.identifier from OAIRecord rc where rc.snapshot.id = ?1 and rc.status = ?2")
	Page<String> listIdentifiersBySnapshotAndStatus(Long snapshotId, RecordStatus status, Pageable pageable);

	Long countBySnapshotAndStatusIn(NetworkSnapshot snapshot, RecordStatus[] status);

	Long countBySnapshotAndTransformed(NetworkSnapshot snapshot, boolean b);
}
