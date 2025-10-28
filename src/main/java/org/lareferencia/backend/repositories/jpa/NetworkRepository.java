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

/**
 * JPA Repository for {@link Network} entity.
 * <p>
 * Provides CRUD operations and custom query methods for managing network configurations.
 * This repository supports REST operations through Spring Data REST.
 * <p>
 * Custom query methods include:
 * </p>
 * <ul>
 *   <li>Finding networks by publication status</li>
 *   <li>Searching networks by name, acronym, or institution</li>
 *   <li>Retrieving networks by snapshot status</li>
 *   <li>Bulk operations for status updates</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see Network
 */
@RepositoryRestResource(path = "network", collectionResourceRel = "network")
public interface NetworkRepository extends JpaRepository<Network, Long> {

	/**
	 * Finds networks by published status ordered by name.
	 *
	 * @param published the published status
	 * @return list of networks
	 */
	List<Network> findByPublishedOrderByNameAsc(boolean published);

	/**
	 * Finds a network by its acronym.
	 *
	 * @param acronym the network acronym
	 * @return the network
	 */
	Network findByAcronym(String acronym);

	/**
	 * Finds networks by name containing the search term (case insensitive).
	 *
	 * @param name the search term
	 * @param pageable pagination information
	 * @return page of networks
	 */
	Page<Network> findByNameIgnoreCaseContaining(String name, Pageable pageable);

	/**
	 * Finds networks by institution name containing the search term (case insensitive).
	 *
	 * @param institution the search term
	 * @param pageable pagination information
	 * @return page of networks
	 */
	Page<Network> findByInstitutionNameIgnoreCaseContaining(String institution, Pageable pageable);

	/**
	 * Finds networks by acronym containing the filter expression (case insensitive).
	 *
	 * @param filterExpression the filter expression
	 * @param pageRequest pagination request
	 * @return page of networks
	 */
	Page<Network> findByAcronymIgnoreCaseContaining(String filterExpression, Pageable pageRequest);
		
	/**
	 * Finds networks filtered by acronym list with pagination.
	 *
	 * @param whiteList list of allowed acronyms
	 * @param pageRequest pagination request
	 * @return page of networks
	 */
	@Query("select n from Network n where n.acronym in (:list)")
	Page<Network> findFilteredByAcronymListPaginated(@Param("list") List<String> whiteList, Pageable pageRequest);
	
	/**
	 * Finds networks filtered by acronym list.
	 *
	 * @param whiteList list of allowed acronyms
	 * @return list of networks
	 */
	@Query("select n from Network n where n.acronym in (:list)")
	List<Network> findFilteredByAcronymList(@Param("list") List<String> whiteList);
	
	/**
	 * Deletes a network by its ID.
	 *
	 * @param network_id the network ID to delete
	 */
	@Modifying
	@Transactional
	@Query("delete from Network n where n.id = ?1")
	void deleteByNetworkID(Long network_id);

	/**
	 * Finds networks by ID with pagination.
	 *
	 * @param filterExpression the filter expression
	 * @param pageRequest pagination request
	 * @return page of networks
	 */
	Page<Network> findById(String filterExpression, Pageable pageRequest);

	/**
	 * Finds networks with custom status filtering.
	 *
	 * @param status the snapshot status to filter by
	 * @param pageRequest pagination request
	 * @return page of networks
	 */
	// ns.status = :status and
	// ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.status = 4 and s.deleted = false)"
	@Query("select ns.network from NetworkSnapshot ns where ns.status = :status and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.deleted = false)")
	Page<Network> customFindByStatus(@Param("status") SnapshotStatus status, Pageable pageRequest);

	/**
	 * Finds networks with custom index status filtering.
	 *
	 * @param status the snapshot index status to filter by
	 * @param pageRequest pagination request
	 * @return page of networks
	 */
	@Query("select ns.network from NetworkSnapshot ns where ns.indexStatus = :status and ns.deleted = false and ns.endTime >= (select max(s.endTime) from NetworkSnapshot s where s.network.id = ns.network.id and s.deleted = false)")
	Page<Network> customFindByIndexStatus(@Param("status") SnapshotIndexStatus status, Pageable pageRequest);

}
