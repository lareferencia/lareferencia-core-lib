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
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Transformer rule that removes field occurrences whose content matches entries
 * in a blacklist.
 * Used to filter out unwanted values from metadata fields.
 */
@ValidatorRuleMeta(name = "Remover ocurrencias por blacklist", help = "Transformer rule that removes field occurrences whose content matches entries in a blacklist.")
public class RemoveBlacklistOccrsRule extends AbstractTransformerRule {

	/**
	 * The name of the field whose occurrences should be checked against the
	 * blacklist.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	@SchemaProperty(title = "Nombre del campo", description = "Campo del cual remover valores en blacklist.", order = 1)
	String fieldName;

	/**
	 * List of blacklisted values. Field occurrences matching these values will be
	 * removed.
	 */
	@Setter
	@Getter
	@JsonProperty("blacklist")
	@SchemaProperty(title = "Blacklist", description = "Lista de valores a remover.", order = 2)
	protected List<String> blacklist;

	/**
	 * Constructs a new remove blacklist occurrences rule with an empty blacklist.
	 */
	public RemoveBlacklistOccrsRule() {
		blacklist = new ArrayList<String>();
	}

	/**
	 * Internal list of nodes marked for removal during transformation.
	 */
	List<Node> removeList = new ArrayList<Node>();

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		removeList.clear();

		boolean wasTransformed = false;

		// recorre las ocurrencias del campo de test
		for (Node node : metadata.getFieldNodes(fieldName)) {

			String occr = node.getFirstChild().getNodeValue();

			if (blacklist.contains(occr)) {
				wasTransformed = true;
				removeList.add(node);
			}
		}

		for (Node node : removeList)
			metadata.removeNode(node);

		return wasTransformed;
	}

}
