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

package org.lareferencia.core.oabroker;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.transaction.annotation.Transactional;

@RepositoryRestResource(path = "broker_event", collectionResourceRel = "event")
public interface BrokerEventRepository extends JpaRepository<BrokerEvent, Long> {


	@Modifying
	@Transactional
	@Query("delete from BrokerEvent n where n.networkId = ?1")
	void deleteByNetworkID(Long network_id);
	
	Page<BrokerEvent> findByNetworkId(Long networkId, Pageable page);
	Page<BrokerEvent> findByNetworkIdAndIdentifier(Long networkId, String identifier, Pageable page);
	Page<BrokerEvent> findByNetworkIdAndIdentifierAndTopic(Long networkId, String identifier, String topic, Pageable page);
	Page<BrokerEvent> findByNetworkIdAndTopic(Long id, String topic, Pageable pageable);
		
	
}
