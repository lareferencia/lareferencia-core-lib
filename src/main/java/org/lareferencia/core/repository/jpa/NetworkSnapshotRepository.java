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

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.SnapshotStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA repository for managing {@link NetworkSnapshot} entities.
 * Provides comprehensive CRUD operations and specialized queries for network snapshots,
 * including status tracking, date range queries, and statistical aggregations.
 */
@RepositoryRestResource(path = "snapshot", collectionResourceRel = "snapshot")
public interface NetworkSnapshotRepository extends JpaRepository<NetworkSnapshot, Long> {

	/**
	 * Finds the last good known snapshot for a network.
	 *
	 * @param networkID the network ID
	 * @return the last good known snapshot
	 */
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ns.status = 9 and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id and s.status = 9 and s.deleted = false)")
	NetworkSnapshot findLastGoodKnowByNetworkID(@Param("network_id") Long networkID);

	/**
	 * Finds the last harvested snapshot for a network.
	 *
	 * @param networkID the network ID
	 * @return the last harvested snapshot
	 */
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ( ns.status = 4 or ns.status = 9 ) and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id and (s.status = 4 OR s.status = 9) and s.deleted = false)")
	NetworkSnapshot findLastHarvestedByNetworkID(@Param("network_id") Long networkID);
	
	/**
	 * Finds the last snapshot for a network regardless of status.
	 *
	 * @param networkID the network ID
	 * @return the last snapshot
	 */
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id)")
	NetworkSnapshot findLastByNetworkID(@Param("network_id") Long networkID);

	/**
	 * Finds snapshots by network and deleted status.
	 *
	 * @param network the network
	 * @param deleted the deleted status
	 * @return list of matching snapshots
	 */
	List<NetworkSnapshot> findByNetworkAndDeleted(Network network, Boolean deleted);
	
	/**
	 * Finds IDs of non-deleted snapshots for a network.
	 *
	 * @param networkId the network ID
	 * @return list of snapshot IDs
	 */
	@Query("select ns.id from NetworkSnapshot ns where ns.network.id = :networkId and ns.deleted = false")
	List<Long> findNonDeletedIdsByNetworkId(Long networkId);

	/**
	 * Finds all snapshot IDs for a network.
	 *
	 * @param networkId the network ID
	 * @return list of all snapshot IDs
	 */
	@Query("select ns.id from NetworkSnapshot ns where ns.network.id = :networkId")
	List<Long> findAllIdsByNetworkId(Long networkId);
	
	/**
	 * Finds snapshots by network with pagination.
	 *
	 * @param network the network
	 * @param pageable pagination information
	 * @return page of snapshots
	 */
	Page<NetworkSnapshot> findByNetwork(Network network, Pageable pageable);

	//List<NetworkSnapshot> findByNetworkAndStatus(Network network, SnapshotStatus status);

	/**
	 * Finds snapshots by network and status with pagination.
	 *
	 * @param network the network
	 * @param valid the snapshot status
	 * @param pageable pagination information
	 * @return page of snapshots
	 */
	Page<NetworkSnapshot> findByNetworkAndStatus(Network network, SnapshotStatus valid, Pageable pageable);

	/**
	 * Finds snapshots by network, status and time range with pagination.
	 *
	 * @param network the network
	 * @param status the snapshot status
	 * @param from start date/time
	 * @param to end date/time
	 * @param pageable pagination information
	 * @return page of snapshots
	 */
	Page<NetworkSnapshot> findByNetworkAndStatusAndEndTimeBetween(Network network, SnapshotStatus status, LocalDateTime from, LocalDateTime to, Pageable pageable);

	/**
	 * Finds snapshots by network and status ordered by end time ascending.
	 *
	 * @param network the network
	 * @param status the snapshot status
	 * @return list of snapshots
	 */
	List<NetworkSnapshot> findByNetworkAndStatusOrderByEndTimeAsc(Network network, SnapshotStatus status);

	/**
	 * Finds snapshots by network and status ordered by end time descending with pagination.
	 *
	 * @param network the network
	 * @param status the snapshot status
	 * @param page pagination information
	 * @return page of snapshots
	 */
	Page<NetworkSnapshot> findByNetworkAndStatusOrderByEndTimeDesc(Network network, SnapshotStatus status, Pageable page);

	/**
	 * Finds snapshots by network ordered by end time ascending.
	 *
	 * @param network the network
	 * @return list of snapshots
	 */
	List<NetworkSnapshot> findByNetworkOrderByEndTimeAsc(Network network);

	/**
	 * Finds snapshots by status ordered by end time ascending.
	 *
	 * @param status the snapshot status
	 * @return list of snapshots
	 */
	List<NetworkSnapshot> findByStatusOrderByEndTimeAsc(SnapshotStatus status);

	/**
	 * Finds snapshots by network ID ordered by start time descending with pagination.
	 *
	 * @param network_id the network ID
	 * @param page pagination information
	 * @return page of snapshots
	 */
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id order by ns.startTime desc")
	Page<NetworkSnapshot> findByNetworkIdOrderByStartTimeDesc(@Param("network_id") Long network_id, Pageable page);
	
	/**
	 * Finds snapshots by network ID ordered with pagination.
	 *
	 * @param network_id the network ID
	 * @param page pagination information
	 * @return page of snapshots
	 */
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id order by ns.startTime desc")
	Page<NetworkSnapshot> findByNetworkIdOrdered(@Param("network_id") Long network_id,  Pageable page);

	/**
	 * Deletes a snapshot by its ID.
	 *
	 * @param snapshot_id the snapshot ID to delete
	 */
	@Modifying
	@Transactional
	@Query("delete from NetworkSnapshot ns where ns.id = ?1")
	void deleteBySnapshotID(Long snapshot_id);
	
	/**
	 * Deletes all snapshots for a network.
	 *
	 * @param network_id the network ID
	 */
	@Modifying
	@Transactional
	@Query("delete from NetworkSnapshot ns where ns.network.id = ?1")
	void deleteByNetworkID(Long network_id);


	/**
	 * Copies non-deleted records from one snapshot to another for incremental harvests.
	 * <p>
	 * This method is used to copy records from the original snapshot to a new snapshot
	 * resulting from an incremental harvest. It copies all records that are not deleted
	 * and that are not already present in the new snapshot.
	 * </p>
	 *
	 * @param fromSnapshot the source snapshot ID
	 * @param toSnapshot the target snapshot ID
	 */
	@Modifying
	@Transactional
	@Query( value = "insert into oairecord (datestamp, identifier, originalmetadatahash, publishedmetadatahash, snapshot_id, status,transformed) " +
			"select o.datestamp , o.identifier, o.originalmetadatahash, o.publishedmetadatahash, :toSnapshot, o.status, o.transformed from oairecord o where o.snapshot_id = :fromSnapshot and " +
			"not exists ( select r.identifier from oairecord r where r.snapshot_id = :toSnapshot and r.identifier = o.identifier )", nativeQuery = true)
	void copyNotDeletedRecordsFromSnapshot(@Param("fromSnapshot")  Long fromSnapshot, @Param("toSnapshot")  Long toSnapshot);



}
