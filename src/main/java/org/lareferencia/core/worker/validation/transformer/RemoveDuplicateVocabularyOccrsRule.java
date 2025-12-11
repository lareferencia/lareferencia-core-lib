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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
 * Transformer rule that removes duplicate occurrences of vocabulary terms in a
 * metadata field,
 * keeping only the first occurrence according to vocabulary priority order.
 * Useful for normalizing controlled vocabulary fields that may contain repeated
 * values.
 */
@ValidatorRuleMeta(name = "Remover ocurrencias redundantes de un vocabulario", help = "Transformer rule that removes duplicate occurrences of vocabulary terms in a metadata field.")
public class RemoveDuplicateVocabularyOccrsRule extends AbstractTransformerRule {

	/**
	 * Name of the metadata field to process for duplicate vocabulary removal.
	 */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	@SchemaProperty(title = "Nombre del campo", description = "Campo del cual remover duplicados de vocabulario.", order = 1)
	String fieldName;

	/**
	 * Ordered list of vocabulary terms used for priority-based duplicate removal.
	 */
	@Setter
	@Getter
	@JsonProperty("vocabulary")
	@SchemaProperty(title = "Vocabulario", description = "Lista ordenada de términos del vocabulario.", order = 2)
	protected List<String> vocabulary;

	/**
	 * Constructs a new RemoveDuplicateVocabularyOccrsRule with an empty vocabulary
	 * list.
	 */
	public RemoveDuplicateVocabularyOccrsRule() {
		vocabulary = new ArrayList<String>();
	}

	/**
	 * Internal list storing pairs of vocabulary indices and corresponding DOM nodes
	 * for tracking duplicate occurrences during transformation.
	 */
	List<Pair<Integer, Node>> vocabularyOccurreces = new ArrayList<Pair<Integer, Node>>();

	/**
	 * Transforms the record by removing duplicate vocabulary occurrences from the
	 * specified field,
	 * preserving only the first occurrence based on vocabulary order priority.
	 *
	 * @param record   the OAI record being processed
	 * @param metadata the record's metadata containing the field to transform
	 * @return true if any duplicate occurrences were removed, false otherwise
	 */
	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		vocabularyOccurreces.clear();

		boolean wasTransformed = false;

		// recorre las ocurrencias del campo de test
		for (Node node : metadata.getFieldNodes(fieldName)) {

			String occr = node.getFirstChild().getNodeValue();

			// Si la ocurrencia está en el vocabulario se obtiene el numero de orden
			int foundAtindex = vocabulary.indexOf(occr);

			if (foundAtindex != -1) // se crea una entrada con el numero de orden y el nodo
				vocabularyOccurreces.add(new ImmutablePair<Integer, Node>(foundAtindex, node));

		}

		if (vocabularyOccurreces.size() > 1) { // solo si hay repeticiones

			// ordena la lista
			vocabularyOccurreces.sort(Comparator.comparing(Pair::getLeft));
			vocabularyOccurreces.remove(0); // quita la primera aparitción para asegurar que la ocurrencia con mas
											// prioridad no sea borrada

			for (Pair<Integer, Node> vocOccr : vocabularyOccurreces) { // remueve el resto de las occurrencias
				metadata.removeNode(vocOccr.getRight());
			}

			wasTransformed = true;
		}

		return wasTransformed;
	}

}
