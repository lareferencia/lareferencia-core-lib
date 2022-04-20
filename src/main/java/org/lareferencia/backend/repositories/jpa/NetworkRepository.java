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

import java.util.List;

import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.SnapshotIndexStatus;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

@RepositoryRestResource(path = "network", collectionResourceRel = "network")
public interface NetworkRepository extends JpaRepository<Network, Long> {

	List<Network> findByPublishedOrderByNameAsc(boolean published);

	Network findByAcronym(String acronym);

	Page<Network> findByNameIgnoreCaseContaining(String name, Pageable pageable);

	Page<Network> findByInstitutionNameIgnoreCaseContaining(String institution, Pageable pageable);

	Page<Network> findByAcronymIgnoreCaseContaining(String filterExpression, Pageable pageRequest);
		
	@Query("select n from Network n where n.acronym in (:list)")
	Page<Network> findFilteredByAcronymListPaginated(@Param("list") List<String> whiteList, Pageable pageRequest);
	
	@Query("select n from Network n where n.acronym in (:list)")
	List<Network> findFilteredByAcronymList(@Param("list") List<String> whiteList);
	
	@Modifying
	@Transactional
	@Query("delete from Network n where n.id = ?1")
	void deleteByNetworkID(Long network_id);

	Page<Network> findById(String filterExpression, Pageable pageRequest);

	// ns.status = :status and
	// ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.status = 4 and s.deleted = false)"
	@Query("select ns.network from NetworkSnapshot ns where ns.status = :status and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.deleted = false)")
	Page<Network> customFindByStatus(@Param("status") SnapshotStatus status, Pageable pageRequest);

	@Query("select ns.network from NetworkSnapshot ns where ns.indexStatus = :status and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.deleted = false)")
	Page<Network> customFindByIndexStatus(@Param("status") SnapshotIndexStatus status, Pageable pageRequest);

}
