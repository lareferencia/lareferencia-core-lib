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

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 */
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor
@Table(name = "broker_event" , indexes = { @Index(name = "broker_event_identifier",  columnList="identifier", unique = false),  
										   @Index(name = "broker_network_id",  		 columnList="network_id", unique = false),
										   @Index(name = "broker_topic",  		 	 columnList="topic", unique = false)}  )
@jakarta.persistence.Entity
public class BrokerEvent  {

	@EqualsAndHashCode.Include
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@JsonIgnore
	protected Long id;

	@Column(nullable = false)
	private String identifier;
	
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String message;
	
	private String topic;
	
//	@ManyToOne(fetch = FetchType.LAZY)
//	@JoinColumn(name = "network_id")
//	private Network network;
//	
	@Setter(AccessLevel.NONE)
	@JsonIgnore
	@Column(name = "network_id"/*, insertable = false, updatable = false*/)
	private Long networkId;

	public BrokerEvent(String identifier, String message, String topic, Long network_id) {
		super();
		this.identifier = identifier;
		this.message = message;
		this.topic = topic;
		//this.network = network;
		this.networkId = network_id;
	}
}
