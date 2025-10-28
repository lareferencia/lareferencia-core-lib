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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.lareferencia.core.metadata.OAIMetadataBitstream;

import jakarta.persistence.*;
import java.util.Date;

/**
 * Entity representing a bitstream associated with an OAI record.
 * <p>
 * Stores information about binary files (PDFs, documents, etc.) that are part of harvested records.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Getter
@Setter
@Entity
@JsonIgnoreProperties({})
public class OAIBitstream  {
	
	/**
	 * Composite primary key for the bitstream.
	 */
	@EmbeddedId
	private OAIBitstreamId id;
	
	@Column(nullable = false)
	private String type;
	
	@Column(nullable = false)
	private String filename;
	
	@Column(nullable = false)
	private Integer sid;
	
	@Column(nullable = false)
	private String url;
	
	@Column(nullable = false)
	private String mime;
	
	@Temporal(TemporalType.TIMESTAMP)
	@Column(nullable = false)
	private Date datestamp;
	
	@Column(nullable = false)
	private OAIBitstreamStatus status;
	
	@Column
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String fulltext;
	
	/**
	 * Constructs a new OAI bitstream with default values.
	 * Sets the status to NEW and the datestamp to the current date.
	 */
	public OAIBitstream() {
		super();
		this.status = OAIBitstreamStatus.NEW;
		this.datestamp = new Date();
		
		
	}
	
	/**
	 * Constructs a new OAI bitstream from metadata information.
	 * Creates the bitstream ID from network, identifier and checksum, and updates fields from metadata.
	 * If no checksum is provided, generates one using MD5 hash of the URL.
	 * 
	 * @param network the network this bitstream belongs to
	 * @param identifier the record identifier this bitstream is associated with
	 * @param mdbs the metadata bitstream containing file information
	 */
	public OAIBitstream(Network network, String identifier, OAIMetadataBitstream mdbs) {
		this();
	
		if (  mdbs.getChecksum() == null || mdbs.getChecksum().isEmpty() ) {
			mdbs.setChecksum( DigestUtils.md5Hex(mdbs.getUrl())) ;
		}
		
		this.id = new OAIBitstreamId(network, identifier, mdbs.getChecksum());
		this.updateFromMetadata(mdbs);
	}
	
	/**
	 * Updates this bitstream's fields from metadata information.
	 * Copies filename, URL, SID, type, and MIME type from the metadata.
	 * Defaults to "application/pdf" if no format is specified.
	 * 
	 * @param mdbs the metadata bitstream containing updated file information
	 */
	public void updateFromMetadata(OAIMetadataBitstream mdbs) {
		
		this.filename = mdbs.getName();
		this.url = mdbs.getUrl();
		this.sid = mdbs.getSid();
		this.type = mdbs.getType();
		
		if ( !mdbs.getFormat().isEmpty() )
			this.mime = mdbs.getFormat();
		else 
			this.mime = "application/pdf";
		
	}
}
