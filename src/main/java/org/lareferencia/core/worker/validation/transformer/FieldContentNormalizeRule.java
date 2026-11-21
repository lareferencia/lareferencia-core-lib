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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.IValidatorFieldContentRule;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;
import org.lareferencia.core.worker.validation.SchemaProperty;
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
@ValidatorRuleMeta
public class FieldContentNormalizeRule extends AbstractTransformerRule {

	// Only bypass a lookup whose syntax is known to be valid. Other paths keep their diagnostics.
	private static final Pattern SIMPLE_FIELD = Pattern.compile("[a-zA-Z0-9_-]++(?:\\.[a-zA-Z0-9_*-]++)*+(?::[a-zA-Z0-9_-]++)?");

	@Override
	public String toString() {
		return "FieldContentNormalizeRule [validationRule=" + validationRule + ", fieldName=" + fieldName
				+ ", removeInvalidOccurrences=" + removeInvalidOccurrences
				+ ", removeDuplicatedOccurrences=" + removeDuplicatedOccurrences + "]";
	}

	@SchemaProperty(order = 1)
	private IValidatorFieldContentRule validationRule;

	@SchemaProperty(order = 2)
	private String fieldName;

	@SchemaProperty(defaultValue = "false", order = 3)
	private Boolean removeInvalidOccurrences = false;

	@SchemaProperty(defaultValue = "false", order = 4)
	private Boolean removeDuplicatedOccurrences = false;

	/**
	 * Creates a new field content normalization rule.
	 */
	public FieldContentNormalizeRule() {
	}

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		if (Boolean.FALSE.equals(removeInvalidOccurrences) && Boolean.FALSE.equals(removeDuplicatedOccurrences)
				&& fieldName != null && SIMPLE_FIELD.matcher(fieldName).matches()) {
			return false;
		}

		Set<String> seen = Boolean.TRUE.equals(removeDuplicatedOccurrences) ? new HashSet<>() : null;
		boolean changed = false;
		for (Node node : metadata.getFieldNodes(fieldName)) {
			String value = node.getFirstChild().getNodeValue();
			boolean invalid = removeInvalidOccurrences && !validationRule.validate(value).isValid();
			if (invalid) {
				metadata.removeNode(node);
				changed = true;
			}
			// Invalid values also count as seen. A later occurrence can validate differently.
			if (removeDuplicatedOccurrences && !seen.add(value) && !invalid) {
				metadata.removeNode(node);
				changed = true;
			}
		}
		return changed;
	}
}
