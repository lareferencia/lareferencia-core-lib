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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Transformation rule that removes duplicate field occurrences.
 * <p>
 * This rule identifies and removes duplicate values within a specified field,
 * keeping only the first occurrence of each unique value. This is useful for
 * cleaning up metadata that may have accidentally duplicated values.
 * </p>
 * <p>
 * The comparison is case-sensitive and based on exact string matching of the
 * field content.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
public class RemoveDuplicateOccrsRule extends AbstractTransformerRule {
	
	/**
	 * Name of the metadata field from which to remove duplicate occurrences.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	/**
	 * Constructs a new RemoveDuplicateOccrsRule instance.
	 */
	public RemoveDuplicateOccrsRule() {
	}
	
	/**
	 * Internal set for tracking unique field values during transformation.
	 */
	Set<String> occrSet  = new HashSet<String>();
	
	/**
	 * Internal list for collecting duplicate nodes to be removed.
	 */
	List<Node> removeList = new ArrayList<Node>();

	/**
	 * Transforms the record by removing duplicate occurrences from the specified field.
	 * Keeps only the first occurrence of each unique value.
	 *
	 * @param record the OAI record being processed
	 * @param metadata the record's metadata containing the field to deduplicate
	 * @return true if any duplicates were removed, false otherwise
	 */
	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

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
