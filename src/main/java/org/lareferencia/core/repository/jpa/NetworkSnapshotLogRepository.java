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

package org.lareferencia.core.repository.jpa;

import org.lareferencia.core.domain.NetworkSnapshotLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA repository for managing {@link NetworkSnapshotLog} entities.
 * Provides CRUD operations and custom queries for network snapshot log entries.
 */
@RepositoryRestResource(path = "log", collectionResourceRel = "log", exported = true)
public interface NetworkSnapshotLogRepository extends JpaRepository<NetworkSnapshotLog, Long> {

	/**
	 * Finds snapshot logs by snapshot ID, ordered by ID descending.
	 *
	 * @param snapshot_id the snapshot ID
	 * @param page pagination information
	 * @return page of snapshot logs
	 */
	@Query("select nsl from NetworkSnapshotLog nsl where nsl.snapshot.id = :snapshot_id order by nsl.id desc")
	Page<NetworkSnapshotLog> findBySnapshotId(@Param("snapshot_id") Long snapshot_id, Pageable page);

	/**
	 * Deletes all snapshot logs for the given snapshot ID.
	 *
	 * @param snapshot_id the snapshot ID
	 */
	@Modifying
	@Transactional
	@Query("delete from NetworkSnapshotLog nsl where nsl.snapshot.id = ?1")
	void deleteBySnapshotID(Long snapshot_id);

}
