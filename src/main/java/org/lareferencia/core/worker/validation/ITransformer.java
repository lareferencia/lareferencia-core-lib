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

import java.util.List;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.NetworkRunningContext;

/**
 * Interface for metadata transformers.
 * <p>
 * Transformers apply a list of transformation rules to modify
 * metadata records according to configured rules.
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface ITransformer {

	/**
	 * Gets the list of transformation rules.
	 * 
	 * @return the list of rules
	 */
	public List<ITransformerRule> getRules();

	/**
	 * Sets the list of transformation rules.
	 * 
	 * @param validators the list of rules to apply
	 */
	public void setRules(List<ITransformerRule> validators);

	/**
	 * Transforms the OAI record.
	 * 
	 * @param record the OAI record to transform
	 * @param metadata the record metadata
	 * @return true if any transformation was applied, false otherwise
	 * @throws ValidationException if transformation fails
	 */
	public boolean transform(NetworkRunningContext networkContext, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException;
}
