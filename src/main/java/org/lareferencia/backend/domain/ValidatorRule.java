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

package org.lareferencia.backend.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import lombok.Getter;
import lombok.Setter;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.lareferencia.core.validation.QuantifierValues;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JPA entity representing a single validation rule within a {@link Validator}.
 * <p>
 * Each ValidatorRule defines a specific validation check that can be applied to
 * metadata records. Rules can be mandatory or optional and specify quantifier
 * constraints (e.g., ONE_OR_MORE, ZERO_OR_MORE) for field occurrences.
 * </p>
 * <p>
 * The rule logic is stored as a JSON serialization, allowing for flexible and
 * extensible rule definitions that can be deserialized into specific rule
 * implementations at runtime.
 * </p>
 * 
 * @author LA Referencia Team
 * @see Validator
 * @see QuantifierValues
 */
@Entity
@Getter
@Setter
public class ValidatorRule {
	
	/** Unique identifier for the validator rule. */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	/**
	 * Constructs a new ValidatorRule instance.
	 */
	public ValidatorRule() {
	}

	/**
	 * The name of the validator rule.
	 */
	@Column(nullable = false)
	private String name;

	/**
	 * Optional description of the validator rule.
	 */
	@Column(nullable = true)
	private String description;

	/**
	 * Whether this validation rule is mandatory.
	 */
	@Column(nullable = false)
	protected Boolean mandatory = false;

	/**
	 * The quantifier specifying how many occurrences are expected.
	 */
	@Column(nullable = false)
	protected QuantifierValues quantifier = QuantifierValues.ONE_OR_MORE;

	/**
	 * JSON serialization of the validator rule configuration.
	 */
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String jsonserialization;

}
