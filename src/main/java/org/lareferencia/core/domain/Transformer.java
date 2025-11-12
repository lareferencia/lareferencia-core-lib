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
 * JPA entity representing a transformation configuration for metadata records.
 * <p>
 * A Transformer contains a collection of {@link TransformerRule} instances that define
 * metadata transformation operations. Transformers are associated with {@link Network}
 * entities and can be used as primary or secondary transformers in the processing pipeline.
 * </p>
 * <p>
 * Transformation rules modify, enrich, or restructure metadata to conform to target
 * schemas, add provenance information, or perform data quality improvements. Rules are
 * applied in a defined sequence based on their order.
 * </p>
 * 
 * @author LA Referencia Team
 * @see TransformerRule
 * @see Network
 */
@Entity
@Getter
@Setter
public class Transformer  {
	
	/** Unique identifier for the transformer. */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	/** Name of the transformer. */
	@Column(nullable = false)
	private String name;

	/** Description of the transformer's purpose and functionality. */
	@Column(nullable = true)
	private String description;

	/** Ordered list of transformation rules to apply. */
	@OneToMany(cascade = CascadeType.ALL)
	@JoinColumn(name = "transformer_id")
	@LazyCollection(LazyCollectionOption.FALSE)
	private List<TransformerRule> rules = new ArrayList<TransformerRule>();

	/**
	 * Constructs a new Transformer with an empty rules list.
	 */
	public Transformer() {
		super();
		rules = new ArrayList<TransformerRule>();
	}

	/**
	 * Resets the ID of this transformer and all associated rules.
	 * <p>
	 * Useful when creating a copy or clone of a transformer configuration
	 * to ensure new database entries are created.
	 * </p>
	 */
	public void resetId() {
		this.id = null;
		for (TransformerRule rule : rules)
			rule.setId(null);
	}
}
