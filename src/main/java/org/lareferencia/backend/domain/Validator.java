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

import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import lombok.Getter;
import lombok.Setter;

/**
 * JPA entity representing a validation configuration for metadata records.
 * <p>
 * A Validator contains a collection of {@link ValidatorRule} instances that define
 * the validation criteria applied to harvested metadata records. Validators are
 * associated with {@link Network} entities and can be used as either pre-validators
 * or main validators in the processing pipeline.
 * </p>
 * <p>
 * The validation rules are applied sequentially to check metadata quality,
 * completeness, and conformance to specific standards or requirements.
 * </p>
 * 
 * @author LA Referencia Team
 * @see ValidatorRule
 * @see Network
 */
@Entity
@Getter
@Setter
public class Validator {

	/** Unique identifier for the validator. */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	/** Name of the validator. */
	@Column(nullable = false)
	private String name;

	/** Description of the validator's purpose and functionality. */
	@Column(nullable = true)
	private String description;

	/** Ordered list of validation rules to apply. */
	@OneToMany(cascade = CascadeType.ALL)
	@JoinColumn(name = "validator_id")
	@LazyCollection(LazyCollectionOption.FALSE)
	private List<ValidatorRule> rules = new ArrayList<ValidatorRule>();

	/**
	 * Constructs a new Validator with an empty rules list.
	 */
	public Validator() {
		super();
		rules = new ArrayList<ValidatorRule>();
	}

	/**
	 * Resets the ID of this validator and all associated rules.
	 * <p>
	 * Useful when creating a copy or clone of a validator configuration
	 * to ensure new database entries are created.
	 * </p>
	 */
	public void resetId() {
		this.id = null;
		for (ValidatorRule rule : rules)
			rule.setId(null);
	}
}


