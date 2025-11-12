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

package org.lareferencia.core.worker.validation.transformer;

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Transformer rule that removes all occurrences of a field except the first one.
 * Useful for ensuring single-valued fields when duplicates exist.
 */
public class RemoveAllButFirstOccrRule extends AbstractTransformerRule {
	
	/**
	 * Name of the metadata field to process, keeping only the first occurrence.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	/**
	 * Constructs a new RemoveAllButFirstOccrRule instance.
	 */
	public RemoveAllButFirstOccrRule() {
	}

	/**
	 * Transforms the record by removing all but the first occurrence of the specified field.
	 *
	 * @param record the OAI record being processed
	 * @param metadata the record's metadata containing the field to transform
	 * @return true if any occurrences were removed, false otherwise
	 */
	@Override
	public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) {

		boolean wasTransformed = false;
		
		List<Node> removeList = new ArrayList<Node>();
		
		int i = 0;
		// recorre las ocurrencias del campo de test
		for ( Node node : metadata.getFieldNodes(fieldName) ) {

			if ( i!=0 ) {
				removeList.add(node);
				wasTransformed = true;
			}
			i++;
		}
			
		for (Node node: removeList) 
			metadata.removeNode(node);
			

		return wasTransformed;
	}

}
