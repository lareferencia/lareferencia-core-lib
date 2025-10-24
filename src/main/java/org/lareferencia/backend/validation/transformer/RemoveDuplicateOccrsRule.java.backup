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

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

public class RemoveDuplicateOccrsRule extends AbstractTransformerRule {
	
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	public RemoveDuplicateOccrsRule() {
	}
	
	Set<String> occrSet  = new HashSet<String>();
	List<Node> removeList = new ArrayList<Node>();

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {

		boolean wasTransformed = false;
		
		occrSet.clear();
		removeList.clear();
		
		// recorre las ocurrencias del campo de test
		for ( Node node : metadata.getFieldNodes(fieldName) ) {

			String occr = node.getFirstChild().getNodeValue();

			if ( occrSet.contains(occr) ) {
				removeList.add(node);
				wasTransformed = true;
			}
			else
				occrSet.add(occr);
		}
			
		for (Node node: removeList) 
			metadata.removeNode(node);
			

		return wasTransformed;
	}

}
