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

package org.lareferencia.core.validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

import lombok.Getter;
import lombok.Setter;

/**
 * Abstract base class for validation rules.
 * <p>
 * Provides common functionality for all metadata validation rules including
 * rule identification, mandatory/optional status, and field quantifier constraints.
 * </p>
 * <p>
 * Validation rules are serialized to/from JSON for storage and configuration,
 * using Jackson's type information to preserve the concrete rule class.
 * </p>
 * 
 * @author LA Referencia Team
 * @see IValidatorRule
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.PROPERTY, property = "@class")
public abstract class AbstractValidatorRule implements IValidatorRule {

	/**
	 * The unique identifier of this rule in the database.
	 */
	@JsonIgnore
	protected Long ruleId;

	/**
	 * Indicates whether this validation rule is mandatory.
	 * Mandatory rules cause record rejection if validation fails.
	 */
	@JsonIgnore
	protected Boolean mandatory;

	/**
	 * The quantifier defining how many field occurrences are required.
	 * For example, ONE_OR_MORE, ZERO_OR_MORE, EXACTLY_ONE.
	 */
	@JsonIgnore
	protected QuantifierValues quantifier;

	/**
	 * Default constructor initializing rule as non-mandatory with ONE_OR_MORE quantifier.
	 */
	public AbstractValidatorRule() {
		this.mandatory = false;
		this.quantifier = QuantifierValues.ONE_OR_MORE;
	}


}
