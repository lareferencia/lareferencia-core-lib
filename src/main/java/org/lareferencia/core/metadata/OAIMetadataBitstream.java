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

package org.lareferencia.core.metadata;

import org.apache.commons.codec.digest.Md5Crypt;

import lombok.Getter;
import lombok.Setter;


/**
 * Represents metadata information for a bitstream (file) associated with an OAI record.
 * Contains file properties like name, type, URL, format, and checksum.
 */
@Setter
public class OAIMetadataBitstream {

	/**
	 * Constructs a new OAI metadata bitstream with default values.
	 */
	public OAIMetadataBitstream() {
	}
	
	/**
	 * The sequence ID of this bitstream within the record.
	 */
	@Getter
	Integer sid;
	
	/**
	 * The type of bitstream (e.g., "ORIGINAL", "TEXT").
	 */
	@Getter
	String type;
	
	@Getter
	String name;
	
	@Getter
	String format;
	
	@Getter
	String size;
	
	@Getter
	String url;

	String checksum;
	
	
	
	@Override
	public String toString() {
		return "OAIMetadataBundle [name=" + name + ", type=" + type + ", url=" + url + "]";
	}

	/**
	 * Gets the MD5 checksum of the bitstream file.
	 * 
	 * @return the file checksum
	 */
	public String getChecksum() {
		return checksum;
	}
       
	
	
}
