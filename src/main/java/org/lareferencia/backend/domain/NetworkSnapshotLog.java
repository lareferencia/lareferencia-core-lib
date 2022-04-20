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

import java.time.LocalDateTime;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;

import org.hibernate.annotations.Type;
import org.lareferencia.core.util.JsonDateSerializer;
import org.lareferencia.core.util.LocalDateTimeAttributeConverter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * 
 */
@Entity
@Getter
@Setter
@JsonIgnoreProperties({ "snapshot" })
public class NetworkSnapshotLog { 
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	@Setter(AccessLevel.NONE)
	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "snapshot_id", insertable = false, updatable = false)
	private NetworkSnapshot snapshot;
	
	@JsonIgnore
	@Column(name = "snapshot_id")
	private Long snapshotId;

	@Column(nullable = false)
	@Lob
	@Basic(fetch = FetchType.LAZY)
	@Type(type = "org.hibernate.type.TextType")
	private String message;


	@Column(nullable = false)
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	@JsonSerialize(using = JsonDateSerializer.class)
	private LocalDateTime timestamp;

	public NetworkSnapshotLog() {
		super();
	}

	public NetworkSnapshotLog(String message, Long snapshotId) {
		super();

		this.message = message;
		this.snapshotId = snapshotId;
		this.timestamp = LocalDateTime.now();

	}

	
}
