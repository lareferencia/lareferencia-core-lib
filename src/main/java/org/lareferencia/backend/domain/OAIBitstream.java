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
import org.lareferencia.core.metadata.OAIMetadataBitstream;

import javax.persistence.*;
import java.util.Date;

/**
 * 
 */
@Getter
@Setter
@Entity
@JsonIgnoreProperties({})
public class OAIBitstream  {
	
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
	
	@Column(nullable = false, columnDefinition = "TEXT")
	private String fulltext;
	
	public OAIBitstream() {
		super();
		this.status = OAIBitstreamStatus.NEW;
		this.datestamp = new Date();
		
		
	}
	
	public OAIBitstream(Network network, String identifier, OAIMetadataBitstream mdbs) {
		this();
	
		if (  mdbs.getChecksum() == null || mdbs.getChecksum().isEmpty() ) {
			mdbs.setChecksum( DigestUtils.md5Hex(mdbs.getUrl())) ;
		}
		
		this.id = new OAIBitstreamId(network, identifier, mdbs.getChecksum());
		this.updateFromMetadata(mdbs);
	}
	
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
