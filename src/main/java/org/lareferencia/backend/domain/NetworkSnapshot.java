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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;
import org.lareferencia.core.util.JsonDateSerializer;
import org.lareferencia.core.util.LocalDateTimeAttributeConverter;

import java.time.LocalDateTime;

import javax.persistence.*;

/**
 * 
 */
@Entity
@JsonIgnoreProperties({ "network" })
@JsonAutoDetect
public class NetworkSnapshot  {
	
	@Id
	@Getter
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	@Getter
	@Setter
	@Column(nullable = false)
	private SnapshotStatus status;
	
	@Getter
	@Setter
	@Column(nullable = false)
	private SnapshotIndexStatus indexStatus;

	@Getter
	@Setter
	@Column(nullable = false)
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	@JsonSerialize(using = JsonDateSerializer.class)
	private LocalDateTime startTime;
	
	@Setter
	@Getter
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	@JsonSerialize(using = JsonDateSerializer.class)
	private LocalDateTime lastIncrementalTime;

	@Getter
	@Setter
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	@JsonSerialize(using = JsonDateSerializer.class)
	private LocalDateTime endTime;
	
	@Getter
	@Setter
	@Column(nullable = false)
	private Integer size;

	@Getter
	@Setter
	@Column(nullable = false)
	private Integer validSize;

	@Getter
	@Setter
	@Column(nullable = false)
	private Integer transformedSize;

	@Getter
	@Setter
	@Column
	private String resumptionToken;

	@Getter
	@Setter
	@ManyToOne()
	@JoinColumn(name = "network_id"/* , nullable=false */)
	private Network network;

	@Getter
	@Setter
	boolean deleted;

	public NetworkSnapshot() {
		super();
		this.status = SnapshotStatus.INITIALIZED;
		indexStatus = SnapshotIndexStatus.UNKNOWN;
		startTime = LocalDateTime.now();
		this.size = 0;
		this.validSize = 0;
		this.transformedSize = 0;
		this.deleted = false;
	}

	public void incrementSize() {
		size++;
	}

	public void incrementValidSize() {
		validSize++;
	}
	
	public void decrementValidSize() {
		validSize--;
	}

	public void incrementTransformedSize() {
		transformedSize++;
	}
	
	public void decrementTransformedSize() {
		transformedSize--;
	}

//	@JsonSerialize(using = JsonDateSerializer.class)
//	public java.util.Date getStartTime() {
//		return startTime;
//	}
//
//	@JsonSerialize(using = JsonDateSerializer.class)
//	public java.util.Date getEndTime() {
//		return endTime;
//	}

}
