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

package org.lareferencia.core.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import lombok.Getter;
import lombok.Setter;

/**
 * JPA entity representing a single transformation rule within a {@link Transformer}.
 * <p>
 * Each TransformerRule defines a specific metadata transformation operation.
 * Rules are executed in a defined sequence based on the runorder field, allowing
 * complex multi-step transformations to be orchestrated.
 * </p>
 * <p>
 * The transformation logic is stored as a JSON serialization, enabling flexible
 * rule definitions that can be deserialized into specific transformation
 * implementations at runtime.
 * </p>
 * 
 * @author LA Referencia Team
 * @see Transformer
 */
@Entity
@Getter
@Setter
public class TransformerRule  {
	
	/** Unique identifier for the transformer rule. */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	/** Name of the transformation rule. */
	@Column(nullable = false)
	private String name;

	/** Description of the transformation rule's purpose and behavior. */
	@Column(nullable = true)
	private String description;
	
	/** Execution order of this rule within the transformer (lower values execute first). */
	@Column(nullable = false)
	private Integer runorder;

	/** JSON serialization of the transformation rule configuration. */
	@Setter
	@Getter
	@JdbcTypeCode(SqlTypes.LONGVARCHAR)
	private String jsonserialization;

	/**
	 * Constructs a new transformer rule with default values.
	 */
	public TransformerRule() {
		// Default constructor for JPA
	}

	@Override
	public String toString() {
		return "TransformerRule [name=" + name + ", order=" + runorder + "]";
	}
	
	
}
