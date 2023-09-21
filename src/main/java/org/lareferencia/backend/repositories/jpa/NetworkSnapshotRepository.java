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
import java.util.Date;
import java.util.List;

import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

@RepositoryRestResource(path = "snapshot", collectionResourceRel = "snapshot")
public interface NetworkSnapshotRepository extends JpaRepository<NetworkSnapshot, Long> {

	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ns.status = 9 and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id and s.status = 9 and s.deleted = false)")
	NetworkSnapshot findLastGoodKnowByNetworkID(@Param("network_id") Long networkID);

	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ( ns.status = 4 or ns.status = 9 ) and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id and (s.status = 4 OR s.status = 9) and s.deleted = false)")
	NetworkSnapshot findLastHarvestedByNetworkID(@Param("network_id") Long networkID);
	
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = :network_id)")
	NetworkSnapshot findLastByNetworkID(@Param("network_id") Long networkID);

	List<NetworkSnapshot> findByNetworkAndDeleted(Network network, Boolean deleted);
	
	@Query("select ns.id from NetworkSnapshot ns where ns.network.id = :networkId and ns.deleted = false")
	List<Long> findNonDeletedIdsByNetworkId(Long networkId);

	@Query("select ns.id from NetworkSnapshot ns where ns.network.id = :networkId")
	List<Long> findAllIdsByNetworkId(Long networkId);
	
	Page<NetworkSnapshot> findByNetwork(Network network, Pageable pageable);

	//List<NetworkSnapshot> findByNetworkAndStatus(Network network, SnapshotStatus status);

	Page<NetworkSnapshot> findByNetworkAndStatus(Network network, SnapshotStatus valid, Pageable pageable);

	Page<NetworkSnapshot> findByNetworkAndStatusAndEndTimeBetween(Network network, SnapshotStatus status, LocalDateTime from, LocalDateTime to, Pageable pageable);

	List<NetworkSnapshot> findByNetworkAndStatusOrderByEndTimeAsc(Network network, SnapshotStatus status);

	Page<NetworkSnapshot> findByNetworkAndStatusOrderByEndTimeDesc(Network network, SnapshotStatus status, Pageable page);

	List<NetworkSnapshot> findByNetworkOrderByEndTimeAsc(Network network);

	List<NetworkSnapshot> findByStatusOrderByEndTimeAsc(SnapshotStatus status);

	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id order by ns.startTime desc")
	Page<NetworkSnapshot> findByNetworkIdOrderByStartTimeDesc(@Param("network_id") Long network_id, Pageable page);
	
	@Query("select ns from NetworkSnapshot ns where ns.network.id = :network_id order by ns.startTime desc")
	Page<NetworkSnapshot> findByNetworkIdOrdered(@Param("network_id") Long network_id,  Pageable page);

	@Modifying
	@Transactional
	@Query("delete from NetworkSnapshot ns where ns.id = ?1")
	void deleteBySnapshotID(Long snapshot_id);
	
	@Modifying
	@Transactional
	@Query("delete from NetworkSnapshot ns where ns.network.id = ?1")
	void deleteByNetworkID(Long network_id);


	/** This method is used to copy the records from the original snapshot to the new snapshot resulting from an incremental harvest
	 *  It copies all the records that are not deleted and that are not already in the new snapshot
	 * @param fromSnapshot
	 * @param toSnapshot
	 */
	@Modifying
	@Transactional
	@Query( value = "insert into oairecord (datestamp, identifier, originalmetadatahash, publishedmetadatahash, snapshot_id, status,transformed) " +
			"select o.datestamp , o.identifier, o.originalmetadatahash, o.publishedmetadatahash, :toSnapshot, o.status, o.transformed from oairecord o where o.snapshot_id = :fromSnapshot and " +
			"not exists ( select r.identifier from oairecord r where r.snapshot_id = :toSnapshot and r.status = 3 and r.identifier = o.identifier )", nativeQuery = true)
	void copyNotDeletedRecordsFromSnapshot(@Param("fromSnapshot")  Long fromSnapshot, @Param("toSnapshot")  Long toSnapshot);



}
