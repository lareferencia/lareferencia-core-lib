
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

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.w3c.dom.Node;

/**
 * Transformer rule that translates field content using regular expression
 * search and replace.
 * Can copy translated content to a target field and optionally remove matching
 * occurrences from the source.
 */
@ValidatorRuleMeta
public class RegexTranslateRule extends AbstractTransformerRule {

	/**
	 * The name of the source field to read content from.
	 */
	@Setter
	@Getter
	@SchemaProperty(order = 1)
	String sourceFieldName;

	/**
	 * The name of the target field to write translated content to.
	 */
	@Setter
	@Getter
	@SchemaProperty(order = 2)
	String targetFieldName;

	/**
	 * The regular expression pattern to search for in the field content.
	 */
	@Getter
	@SchemaProperty(order = 3)
	String regexSearch;

	/**
	 * The replacement string to use when the regex pattern matches.
	 */
	@Setter
	@Getter
	@SchemaProperty(order = 4)
	String regexReplace;

	/**
	 * Flag indicating whether to remove occurrences that match the regex pattern.
	 */
	@Setter
	@Getter
	@SchemaProperty(defaultValue = "false", order = 5)
	Boolean removeMatchingOccurrences = false;

	/**
	 * Constructs a new regex translate rule with default settings.
	 */
	public RegexTranslateRule() {

	}

	/**
	 * Sets the regular expression search pattern.
	 * 
	 * @param regexPattern the regex pattern to search for
	 */
	public void setRegexSearch(String regexPattern) {
		this.regexSearch = regexPattern;
	}

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		List<Node> sourceNodes = metadata.getFieldNodes(sourceFieldName);
		if (sourceNodes.isEmpty()) {
			return false;
		}

		// Seed from all original source values before changing the DOM, including later siblings.
		Set<String> existingValues = new HashSet<>();
		for (Node node : sourceNodes) {
			existingValues.add(node.getFirstChild().getNodeValue());
		}

		Pattern pattern = Pattern.compile(regexSearch);
		boolean wasTransformed = false;
		for (Node node : sourceNodes) {
			String value = node.getFirstChild().getNodeValue();
			String replacement = pattern.matcher(value).replaceFirst(regexReplace);
			if (existingValues.add(replacement)) {
				if (removeMatchingOccurrences) {
					metadata.removeNode(node);
				}
				metadata.addFieldOcurrence(getTargetFieldName(), replacement);
				wasTransformed = true;
			}
		}
		return wasTransformed;
	}

}
