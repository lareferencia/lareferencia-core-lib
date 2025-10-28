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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.validation.validator.FieldExpressionEvaluator;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.lareferencia.core.validation.QuantifierValues;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transformer rule that removes empty or whitespace-only occurrences from a metadata field.
 * Cleans up fields by eliminating nodes that contain only whitespace or are empty.
 */
public class RemoveEmptyOccrsRule extends AbstractTransformerRule {
	
	/**
	 * Name of the metadata field to remove empty occurrences from.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	/**
	 * Constructs a new RemoveEmptyOccrsRule instance.
	 */
	public RemoveEmptyOccrsRule() {
	}

	/**
	 * Transforms the record by removing all empty or whitespace-only occurrences
	 * from the specified field.
	 *
	 * @param record the OAI record being processed
	 * @param metadata the record's metadata containing the field to clean
	 * @return true if any empty occurrences were removed, false otherwise
	 */
	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {

		boolean wasTransformed = false;
		
		List<Node> removeList = new ArrayList<Node>();
		
		// recorre las ocurrencias del campo de test
		for ( Node node : metadata.getFieldNodes(fieldName) ) {

			String occr = node.getFirstChild().getNodeValue();

			if ( occr.trim().isEmpty() ) {
				removeList.add(node);
				wasTransformed = true;
			}
			
		}
			
		for (Node node: removeList) 
			metadata.removeNode(node);
			

		return wasTransformed;
	}
	

}
