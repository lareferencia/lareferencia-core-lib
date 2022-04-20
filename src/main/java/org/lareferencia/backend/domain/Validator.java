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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import lombok.Getter;
import lombok.Setter;

/**
 * NationalNetwork Entity
 */
@Entity
@Getter
@Setter
public class Validator {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	@Column(nullable = false)
	private String name;

	@Column(nullable = true)
	private String description;

	@OneToMany(cascade = CascadeType.ALL)
	@JoinColumn(name = "validator_id")
	@LazyCollection(LazyCollectionOption.FALSE)
	private List<ValidatorRule> rules = new ArrayList<ValidatorRule>();

	public Validator() {
		super();
		rules = new ArrayList<ValidatorRule>();
	}

	/**
	 * Reset all ids
	 */
	public void resetId() {
		this.id = null;
		for (ValidatorRule rule : rules)
			rule.setId(null);
	}
}


