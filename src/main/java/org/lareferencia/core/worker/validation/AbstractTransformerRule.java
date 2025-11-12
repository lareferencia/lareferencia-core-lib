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

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.NetworkRunningContext;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

/**
 * Abstract base class for transformation rules.
 * <p>
 * Provides the foundation for all metadata transformation rules in the system.
 * Concrete implementations define specific transformation operations that modify,
 * enrich, or restructure metadata records.
 * </p>
 * <p>
 * Transformation rules are serialized to/from JSON for storage and configuration,
 * using Jackson's type information to preserve the concrete rule class.
 * </p>
 * 
 * @author LA Referencia Team
 * @see ITransformerRule
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.PROPERTY, property = "@class")
public abstract class AbstractTransformerRule implements ITransformerRule {

	/**
	 * The unique identifier of this rule in the database.
	 */
	@JsonIgnore
	protected Long ruleId;
	
	/**
	 * Default constructor.
	 */
	public AbstractTransformerRule() {
	}

	/**
	 * Transforms the metadata of the given record.
	 * <p>
	 * Implementations should modify the metadata object in place and return true
	 * if any transformation was actually applied.
	 * </p>
	 * 
	 * @param record the OAI record being processed
	 * @param metadata the metadata to transform
	 * @return true if a transformation was applied, false otherwise
	 * @throws ValidationException if an error occurs during transformation
	 */
	public abstract boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException;
	
}
