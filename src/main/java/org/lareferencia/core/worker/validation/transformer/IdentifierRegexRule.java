
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
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

import java.util.HashSet;
import java.util.Set;

/**
 * Transformation rule that applies regex replacement to record identifiers.
 * <p>
 * Uses regex patterns to modify OAI record identifiers.
 * </p>
 * 
 * @author LA Referencia Team
 */
public class IdentifierRegexRule extends AbstractTransformerRule {

	/**
	 * Regex pattern for searching in identifiers.
	 */
	@Getter
	String regexSearch;

	@Setter
	@Getter
	String regexReplace;


	/**
	 * Creates a new identifier regex rule.
	 */
	public IdentifierRegexRule() {
		
		
	}
	
	/**
	 * Sets the regex search pattern.
	 * 
	 * @param regexPattern the regex pattern to search for
	 */
	public void setRegexSearch(String regexPattern) {
		this.regexSearch = regexPattern;
		//regexPredicate = Pattern.compile(regexPattern).asPredicate();
	}

	
	Set<String> existingValues = new HashSet<String>();
	String occr = null;
	String replace = null;

	/**
	 * Transforms the record identifier using regex replacement.
	 * 
	 * @param record the OAI record to transform
	 * @param metadata the metadata (not used in this rule)
	 * @return true always, as transformation is always applied
	 */
	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		// set the new identifier value as the result of the regex replace
		record.setIdentifier( record.getIdentifier().replaceAll(regexSearch, regexReplace) );
		return true;

	}

}
