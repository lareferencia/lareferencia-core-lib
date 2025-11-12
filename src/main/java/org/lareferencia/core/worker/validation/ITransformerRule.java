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

package org.lareferencia.core.worker.validation;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.NetworkRunningContext;

/**
 * Interface for metadata transformation rules.
 * <p>
 * Defines the contract for rules that transform metadata records.
 * Implementations provide specific transformation logic such as field
 * mapping, value translation, or metadata enrichment.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
public interface ITransformerRule {

	/**
	 * Transforms the metadata of the given record.
	 * 
	 * @param record the OAI record being processed
	 * @param metadata the metadata to transform
	 * @return true if a transformation was applied, false otherwise
	 * @throws ValidationException if an error occurs during transformation
	 */
	abstract boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException;
	
	/**
	 * Gets the unique identifier of this rule.
	 * 
	 * @return the rule ID
	 */
	public Long getRuleId();
	
	/**
	 * Sets the unique identifier of this rule.
	 * 
	 * @param id the rule ID to set
	 */
	public void setRuleId(Long id);

}
