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

import java.util.List;

import org.lareferencia.core.domain.OAIBitstream;
import org.lareferencia.core.domain.OAIBitstreamId;
import org.lareferencia.core.domain.OAIBitstreamStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA repository for managing OAI bitstream entities.
 * Provides query methods for finding and deleting bitstreams by network, identifier, status, and hash.
 */
@RepositoryRestResource(path = "file", collectionResourceRel = "file")
public interface OAIBitstreamRepository extends JpaRepository<OAIBitstream, OAIBitstreamId> {
	
	
	/**
	 * Finds a single bitstream by its content hash.
	 * 
	 * @param hash the content checksum to search for
	 * @return the bitstream with the specified hash, or null if not found
	 */
	@Query("select rc from OAIBitstream rc where rc.id.checksum = ?1")
	OAIBitstream findOneByHash(String hash);
	
	/**
	 * Finds all bitstreams for a specific network and record identifier.
	 * 
	 * @param networkID the network ID
	 * @param identifier the record identifier
	 * @return list of bitstreams matching the criteria
	 */
	@Query("select r from OAIBitstream r where r.id.network.id = ?1 and r.id.identifier = ?2")
	List<OAIBitstream> findByNetworkIdAndIdentifier(Long networkID, String identifier);
	
	/**
	 * Finds bitstreams by network and status with optional status negation.
	 * 
	 * @param networkID the network ID
	 * @param status the bitstream status to filter by
	 * @param negateStatus if true, finds bitstreams NOT matching the status; if false, finds matching status
	 * @param pageable pagination parameters
	 * @return page of bitstreams matching the criteria
	 */
	@Query("select rc from OAIBitstream rc where rc.id.network.id = ?1 and ((false=?3 AND rc.status=?2) OR (true=?3 AND rc.status<>?2))")
	Page<OAIBitstream> findByNetworkIdAndStatus(Long networkID, OAIBitstreamStatus status, Boolean negateStatus, Pageable pageable);
	
	/**
	 * Finds all bitstreams for a specific network with pagination.
	 * 
	 * @param networkID the network ID
	 * @param pageable pagination parameters
	 * @return page of bitstreams for the network
	 */
	@Query("select rc from OAIBitstream rc where rc.id.network.id = ?1")
	Page<OAIBitstream> findByNetworkId(Long networkID, Pageable pageable);


	/**
	 * Deletes all bitstreams for a specific network.
	 * 
	 * @param networkID the network ID whose bitstreams should be deleted
	 */
	@Modifying
	@Transactional
	@Query("delete from OAIBitstream r where r.id.network.id = ?1")
	void deleteByNetworkID(Long networkID);
	
	/**
	 * Deletes all bitstreams for a specific network and record identifier.
	 * 
	 * @param networkID the network ID
	 * @param identifier the record identifier whose bitstreams should be deleted
	 */
	@Modifying
	@Transactional
	@Query("delete from OAIBitstream r where r.id.network.id = ?1 and r.id.identifier = ?2")
	void deleteByNetworkIDAndIdentifier(Long networkID, String identifier);

	

}
