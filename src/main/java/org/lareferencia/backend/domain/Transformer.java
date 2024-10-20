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
 * NationalNetwork Entity
 */
@Entity
@Getter
@Setter
public class Transformer  {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	@Column(nullable = false)
	private String name;

	@Column(nullable = true)
	private String description;

	@OneToMany(cascade = CascadeType.ALL)
	@JoinColumn(name = "transformer_id")
	@LazyCollection(LazyCollectionOption.FALSE)
	private List<TransformerRule> rules = new ArrayList<TransformerRule>();

	public Transformer() {
		super();
		rules = new ArrayList<TransformerRule>();
	}

	/**
	 * Reset all ids
	 */
	public void resetId() {
		this.id = null;
		for (TransformerRule rule : rules)
			rule.setId(null);
	}
}
