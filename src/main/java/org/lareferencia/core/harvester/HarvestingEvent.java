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

package org.lareferencia.core.harvester;

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.core.metadata.OAIRecordMetadata;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a harvesting event with records and status information.
 * <p>
 * Contains harvested records, deleted identifiers, and event metadata.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Getter
@Setter
@ToString
public class HarvestingEvent {

	
	/**
	 * List of harvested records in this event.
	 */
	private List<OAIRecordMetadata> records;
	
	/**
	 * List of identifiers for deleted records.
	 */
	private List<String> deletedRecordsIdentifiers;
	
	/**
	 * List of identifiers for missing records.
	 */
	private List<String> missingRecordsIdentifiers;
	
	private String message;
	private String originURL;
	private HarvestingEventStatus status;
	private String resumptionToken;
	private String metadataPrefix;
	private boolean recordMissing = false;
	
	/**
	 * Creates a new harvesting event with empty record lists.
	 */
	public HarvestingEvent() {
		this.records = new ArrayList<OAIRecordMetadata>(100);
		this.deletedRecordsIdentifiers = new ArrayList<String>();
		this.missingRecordsIdentifiers = new ArrayList<String>();
	}
	

	/**
	 * Clears all event data for reuse.
	 */
	public void reset() {
		
		this.records.clear();
		this.missingRecordsIdentifiers.clear();
		this.deletedRecordsIdentifiers.clear();
		
		message = null;
		originURL = null;;
		status = null;
		resumptionToken=null;
		metadataPrefix=null;
		recordMissing = false;
	
	}



}
