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

import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.backend.domain.IOAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.lareferencia.core.validation.IValidatorFieldContentRule;
import org.w3c.dom.Node;

/**
 * Transformation rule that normalizes field content.
 * <p>
 * Can remove invalid occurrences based on a validation rule,
 * and/or remove duplicated values from metadata fields.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
@Getter
@Setter
public class FieldContentNormalizeRule extends AbstractTransformerRule {

	@Override
	public String toString() {
		return "FieldContentNormalizeRule [validationRule=" + validationRule + ", fieldName=" + fieldName + ", removeInvalidOccurrences=" + removeInvalidOccurrences
				+ ", removeDuplicatedOccurrences=" + removeDuplicatedOccurrences + "]";
	}

	private IValidatorFieldContentRule validationRule;

	private String fieldName;

	private Boolean removeInvalidOccurrences = false;
	private Boolean removeDuplicatedOccurrences = false;

	/**
	 * Creates a new field content normalization rule.
	 */
	public FieldContentNormalizeRule() {
	}

	@Override
	public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) {

		ContentValidatorResult result;
		boolean wasTransformed = false;
		Set<String> occurencesHistory = new HashSet<String>();

		// Ciclo de búsqueda
		for (Node node : metadata.getFieldNodes(fieldName)) {

			String occr = node.getFirstChild().getNodeValue();

			if (removeInvalidOccurrences) {

				result = validationRule.validate(occr);

				wasTransformed |= !result.isValid();

				if (!result.isValid()) {
					metadata.removeNode(node);
				}
			}

			if (removeDuplicatedOccurrences) {

				if (occurencesHistory.contains(occr)) {
					wasTransformed |= true;
					metadata.removeNode(node);
				}

				occurencesHistory.add(occr);

			}

		}

		return wasTransformed;
	}

}
