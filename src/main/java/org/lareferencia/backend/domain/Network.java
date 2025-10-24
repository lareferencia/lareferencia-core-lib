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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Transient;

import org.lareferencia.core.util.ListAttributeConverter;
import org.lareferencia.core.util.MapAttributeConverter;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * NationalNetwork Entity
 */
@Entity
@Getter
@ToString
public class Network {
	
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id = null;
	
	@Setter
	private Boolean published;
		
	@Setter
	@Column(nullable = false, length = 20, unique = true)
	private String acronym;
	
	@Setter
	@Column(nullable = false)
	private String name;
	
	@Setter
	@Column(nullable = true)
	private String institutionAcronym;
	
	@Setter
	@Column(nullable = false)
	private String institutionName;
	
	@Setter
	private String metadataPrefix = "oai_dc";
	
	@Setter
	private String metadataStoreSchema = "xoai";

	@Setter
	private String originURL;

	@Setter
	@Column(name="attributes", columnDefinition="TEXT")
	@Convert(converter = MapAttributeConverter.class)
	private Map<String, Object> attributes;	
	
	@Setter
	@Column(name="sets", columnDefinition="TEXT")
	@Convert(converter = ListAttributeConverter.class)
	private List<String> sets = new ArrayList<String>();
	
	@Setter
	@Column(name="properties", columnDefinition="TEXT")
	@Convert(converter = MapAttributeConverter.class)
	private Map<String, Boolean> properties = new HashMap<String, Boolean>();
	
	@Setter
	@OneToMany(cascade = CascadeType.ALL /*, orphanRemoval=true*/, fetch = FetchType.LAZY)
	@JoinColumn(name = "network_id")
	private Collection<NetworkSnapshot> snapshots = new LinkedHashSet<NetworkSnapshot>();

	@Setter
	private String scheduleCronExpression;

	@Getter
	@Setter
	@ManyToOne
	@JoinColumn(name = "pre_validator_id", nullable = true)
	private Validator prevalidator;
	
	@Getter
	@Setter
	@ManyToOne
	@JoinColumn(name = "validator_id", nullable = true)
	private Validator validator;

	@Getter
	@Setter
	@ManyToOne
	@JoinColumn(name = "transformer_id", nullable = true)
	private Transformer transformer;
	
	@Getter
	@Setter
	@ManyToOne
	@JoinColumn(name = "secondary_transformer_id", nullable = true)
	private Transformer secondaryTransformer;


	/***
	 * Método de ayuda para lectura de propiedade booleanas si la propiedad
	 * existe devuelve su valor o false en otro caso
	 * 
	 * @param propertyName
	 *            nombre de la propiedad
	 * @return
	 ***/
	@Transient
	public Boolean getBooleanPropertyValue(String propertyName) {

		if ( properties != null && properties.containsKey(propertyName) )
			return properties.get(propertyName); 
		
		
		return false;
	}
	
}
