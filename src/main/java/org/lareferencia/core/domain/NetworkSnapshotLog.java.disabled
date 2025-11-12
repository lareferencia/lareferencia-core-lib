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

package org.lareferencia.core.domain;

import java.time.LocalDateTime;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.lareferencia.core.util.JsonDateSerializer;
import org.lareferencia.core.util.LocalDateTimeAttributeConverter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Entity representing a log entry for network snapshot operations.
 * <p>
 * Tracks events and status changes during harvesting snapshot processing.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Entity
@Getter
@Setter
@JsonIgnoreProperties({ "snapshot" })
public class NetworkSnapshotLog { 
	
	/**
	 * The unique identifier for this log entry.
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	/**
	 * The snapshot this log entry belongs to.
	 */
	@Setter(AccessLevel.NONE)
	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "snapshot_id", insertable = false, updatable = false)
	private NetworkSnapshot snapshot;
	
	/**
	 * ID of the snapshot this log belongs to.
	 */
	@JsonIgnore
	@Column(name = "snapshot_id")
	private Long snapshotId;

	/**
	 * Log message content.
	 */
	@Column(nullable = false)
	@Lob
	@Basic(fetch = FetchType.LAZY)
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String message;


	/**
	 * Timestamp when this log entry was created.
	 */
	@Column(nullable = false)
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	@JsonSerialize(using = JsonDateSerializer.class)
	private LocalDateTime timestamp;

	/**
	 * Constructs a new network snapshot log entry.
	 */
	public NetworkSnapshotLog() {
		super();
	}

	/**
	 * Constructs a new network snapshot log entry with message and snapshot ID.
	 *
	 * @param message the log message
	 * @param snapshotId the ID of the snapshot this log belongs to
	 */
	public NetworkSnapshotLog(String message, Long snapshotId) {
		super();

		this.message = message;
		this.snapshotId = snapshotId;
		this.timestamp = LocalDateTime.now();

	}

	
}
