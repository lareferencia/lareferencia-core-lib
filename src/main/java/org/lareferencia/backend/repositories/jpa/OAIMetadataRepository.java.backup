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


import org.lareferencia.backend.domain.OAIMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;



@RepositoryRestResource(path = "metadata", collectionResourceRel = "metadata")
public interface OAIMetadataRepository extends JpaRepository<OAIMetadata, String> {
	
	@Query(value="SELECT EXISTS( SELECT 1 FROM oaimetadata WHERE hash = ?1 )", nativeQuery=true)
	Boolean checkIfExists(String hash);

	//@Transactional(readOnly = true)
	@Query(value="SELECT m.metadata from oaimetadata m where m.hash = ?1", nativeQuery=true)
	String getMetadata(String hash);

	/**
	 * Delete orphan metadata by checking if there is a record that uses it
	 */
	@Modifying
	@Query(value="delete from oaimetadata o where  not exists " +
			  "(select r.id from oairecord r where r.originalmetadatahash = o.hash or r.publishedmetadatahash = o.hash)", nativeQuery=true)
	void deleteOrphanMetadataEntries();

}
