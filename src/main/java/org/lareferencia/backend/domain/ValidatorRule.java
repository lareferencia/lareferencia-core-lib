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

import org.hibernate.annotations.Type;
import org.lareferencia.core.validation.QuantifierValues;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * NationalNetwork Entity
 */
@Entity
@Getter
@Setter
public class ValidatorRule {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;

	@Column(nullable = false)
	private String name;

	@Column(nullable = true)
	private String description;

	@Column(nullable = false)
	protected Boolean mandatory = false;

	@Column(nullable = false)
	protected QuantifierValues quantifier = QuantifierValues.ONE_OR_MORE;

	@Setter
	@Getter
	@Column(nullable = false, columnDefinition = "TEXT")
	private String jsonserialization;

}
