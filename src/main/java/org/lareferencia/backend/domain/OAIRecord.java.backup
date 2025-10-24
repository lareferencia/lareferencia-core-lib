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

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.util.LocalDateTimeAttributeConverter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * 
 */
@Getter
@Entity
@JsonIgnoreProperties({ "originalXML","publishedXML", "snapshot", "datestamp" })
public class OAIRecord  {
	
	private static Logger logger = LogManager.getLogger(OAIRecord.class);
	
	@Id
	@GeneratedValue(generator = "OAIRecordSequenceGenerator")	
	@SequenceGenerator(name="OAIRecordSequenceGenerator",sequenceName="oai_record_id_seq", allocationSize=1000)
	private Long id = null;
		
	@Setter
	@Column(nullable = false)
	private String identifier;
	
	@Setter
	@Column(nullable = false)
	@Convert(converter = LocalDateTimeAttributeConverter.class)
	private LocalDateTime datestamp;


	@Setter
	@Getter
	@Column(length = 32)
	private String originalMetadataHash;
	
	@Setter
	@Getter
	@Column(length = 32)
	private String publishedMetadataHash;


	@Setter
	@Column(nullable = false)
	private RecordStatus status = RecordStatus.UNTESTED;
	
	@Setter
	@Column(nullable = false)
	private Boolean transformed = false;

	@Getter
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "snapshot_id")
	private NetworkSnapshot snapshot;
	
	@JsonIgnore
	@Setter(AccessLevel.NONE)
	@Getter
	@Column(name = "snapshot_id", insertable = false, updatable = false)
	private Long snapshotId;

	public OAIRecord() {
		super();
		this.status = RecordStatus.UNTESTED;
		this.datestamp = LocalDateTime.now();
	}

	public OAIRecord(NetworkSnapshot snapshot) {
		super();
		this.snapshot = snapshot;
		this.status = RecordStatus.UNTESTED;
	}

	@Override
	public String toString() {
		return "OAIRecord{" +
				"id=" + id +
				", identifier='" + identifier + '\'' +
				'}';
	}
}
