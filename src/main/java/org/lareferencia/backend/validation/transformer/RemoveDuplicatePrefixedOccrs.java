
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

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.domain.IOAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.RepositoryNameHelper;
import org.lareferencia.core.validation.AbstractTransformerRule;

/**
 * Transformer rule that removes duplicate occurrences with a specific prefix from a metadata field.
 * Useful for cleaning repository names and other prefixed values that may appear multiple times.
 */
public class RemoveDuplicatePrefixedOccrs extends AbstractTransformerRule {

	/**
	 * Name of the metadata field to remove duplicate prefixed values from.
	 * Defaults to "dc.source.none".
	 */
	@Getter
	@Setter
	private String fieldName = "dc.source.none" ;

	/**
	 * The prefix used to identify values to deduplicate.
	 * Defaults to "instname:".
	 */
	@Getter
	@Setter
	private String prefix = "instname:";
	
	/**
	 * Constructs a new RemoveDuplicatePrefixedOccrs instance with default field and prefix.
	 */
	public RemoveDuplicatePrefixedOccrs() {
	}

	/**
	 * Transforms the record by removing duplicate occurrences of prefixed values
	 * from the specified field using RepositoryNameHelper.
	 *
	 * @param record the OAI record being processed
	 * @param metadata the record's metadata containing the field to deduplicate
	 * @return true if any duplicates were removed, false otherwise
	 */
	@Override
	public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) {
		return RepositoryNameHelper.removeDuplicates(metadata, fieldName, prefix);
	}

}
