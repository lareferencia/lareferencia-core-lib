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

package org.lareferencia.backend.validation.transformer;

import java.util.List;

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Transformer rule that reduces heavy records by removing specified fields.
 * Useful for handling oversized metadata records that exceed processing limits.
 */
public class ReduceHeavyRecords extends AbstractTransformerRule {
	
	/**
	 * Maximum allowed record size (currently not actively used in transformation logic).
	 * Defaults to 1.
	 */
	@Setter
	@Getter
	@JsonProperty("maxRecordSize")
	int maxRecordSize = 1;
	
	/**
	 * List of field names to remove from heavy records to reduce their size.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldsToRemove")
	List<String> fieldsToRemove;
	
	/**
	 * Constructs a new ReduceHeavyRecords instance.
	 */
	public ReduceHeavyRecords() {
	}

	/**
	 * Transforms the record by removing all occurrences of the specified fields.
	 *
	 * @param record the OAI record being processed
	 * @param metadata the record's metadata from which fields will be removed
	 * @return true if any fields were removed, false otherwise
	 */
	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {
		
		boolean wasTransformed = false;
				
		for ( String fieldName : fieldsToRemove ) {
			
			for ( Node node : metadata.getFieldNodes(fieldName) ) { 
				metadata.removeNode(node);
				wasTransformed = true;
			}
		}
	
		
		return wasTransformed;
	}
	 
	

}
