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

package org.lareferencia.backend.domain;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Transient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 */
@Getter
@Entity
@NoArgsConstructor
public class OAIMetadata implements Persistable<String> {
	
	private static Logger logger = LogManager.getLogger(OAIMetadata.class);
	
	@Id
	@Column(nullable = false, length = 32, unique = true)
	private String hash;
	
	@Setter
	@Column(nullable = false)
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String metadata;
	

	public OAIMetadata(String metadata, String hash) {
		super();
		this.metadata = metadata;
		this.hash = hash;
	}


	/**
	 * This is part of Persistable interface, and allows to Hibernate not to look for this id in the database, speeds ups persistences by avoiding unesesary queries for existint UUID.
	 */
	@Override
	@JsonIgnore
	public boolean isNew() {
		return neverPersisted;
	}
	
	/** By default on instance creation, neverPersisted is marked as true */
	@Transient
	@JsonIgnore
	@Setter(AccessLevel.NONE)
	@Getter(AccessLevel.NONE)
	private boolean neverPersisted=true;
	
	
	/** On Entity load neverPersisted  false */ 
	@PostLoad
	void onPostLoad() {
		neverPersisted=false;
	}
	
	/** After the persistence is completed successfully the object is marked as persisted */
	@PostPersist
	void onPostPersist() {
		neverPersisted=false;
	}

	@Override
	public String getId() {
		return hash;
	}

	
}
