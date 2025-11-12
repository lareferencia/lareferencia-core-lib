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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles serialization and deserialization of validation and transformation rules to/from JSON.
 * Uses Jackson ObjectMapper for JSON processing.
 */
@Component
public class RuleSerializer {

//	@Getter
//	private List<IValidatorRule> validatorPrototypes;
//
//	@Getter
//	private List<ITransformerRule> transformerPrototypes;

	// JsonObject Mapper
	private ObjectMapper mapper;

	/**
	 * Constructs a new RuleSerializer and initializes the JSON mapper.
	 */
	public RuleSerializer() {
//		validatorPrototypes = new ArrayList<IValidatorRule>();
//		transformerPrototypes = new ArrayList<ITransformerRule>();
		mapper = new ObjectMapper();

	}

//	@SuppressWarnings("unchecked")
//	/***
//	 * Esta metodo carga en el mapper creado los subtipos de la clase AbstractValidatorRule 
//	 * desde los prototipos provisto por la lista, esto permite serializar y reconocer 
//	 * los objetos desde/hacia JSON
//	 */
//	private void updateObjectMapper() {
//		mapper = new ObjectMapper();
//
//		// Set con las clases de los prototipos declarados
//		Set<Class<? extends AbstractValidatorRule>> aValidationRuleSubTypes = new HashSet<Class<? extends AbstractValidatorRule>>();
//
//		for (IValidatorRule rule : validatorPrototypes) {
//			// TODO: Ojo que esto puede ser problematico si algun de las reglas
//			// no es derivada de AbstractValidationRule
//			aValidationRuleSubTypes.add((Class<? extends AbstractValidatorRule>) rule.getClass());
//		}
//		mapper.registerSubtypes(aValidationRuleSubTypes.toArray(new Class<?>[aValidationRuleSubTypes.size()]));
//
//		// Set con las clases de los prototipos declarados
//		Set<Class<? extends AbstractTransformerRule>> aTransformerRuleSubTypes = new HashSet<Class<? extends AbstractTransformerRule>>();
//
//		for (ITransformerRule rule : transformerPrototypes) {
//			// TODO: Ojo que esto puede ser problematico si algun de las reglas
//			// no es derivada de AbstractRule
//			aTransformerRuleSubTypes.add((Class<? extends AbstractTransformerRule>) rule.getClass());
//		}
//		mapper.registerSubtypes(aTransformerRuleSubTypes.toArray(new Class<?>[aTransformerRuleSubTypes.size()]));
//
//	}

//	public void setValidatorPrototypes(List<IValidatorRule> prototypes) {
//		this.validatorPrototypes = prototypes;
//
//		// Cada vez que la lista de prototipos cambia hay que reconstruir el
//		// mapper
//		updateObjectMapper();
//	}
//
//	public void setTransformerPrototypes(List<ITransformerRule> prototypes) {
//		this.transformerPrototypes = prototypes;
//
//		// Cada vez que la lista de prototipos cambia hay que reconstruir el
//		// mapper
//		updateObjectMapper();
//	}

	/**
	 * Serializes a transformer rule to a JSON string.
	 * 
	 * @param rule the transformer rule to serialize
	 * @return the JSON string representation of the rule, or null if serialization fails
	 */
	public String serializeTransformerToJsonString(ITransformerRule rule) {

		try {
			return mapper.writeValueAsString(rule);
		} catch (JsonProcessingException e) {
			// TODO Serialize rule exceptions
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Serializes a validator rule to a JSON string.
	 * 
	 * @param rule the validator rule to serialize
	 * @return the JSON string representation of the rule, or null if serialization fails
	 */
	public String serializeValidatorToJsonString(IValidatorRule rule) {

		try {
			return mapper.writeValueAsString(rule);
		} catch (JsonProcessingException e) {
			// TODO Serialize rule exceptions
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Deserializes a validator rule from a JSON string.
	 * 
	 * @param jsonString the JSON string to deserialize
	 * @return the deserialized validator rule, or null if deserialization fails
	 */
	public IValidatorRule deserializeValidatorFromJsonString(String jsonString) {

		try {
			AbstractValidatorRule rule =  mapper.readValue(jsonString, AbstractValidatorRule.class);
			
			return rule;
			
		} catch (JsonParseException e) {

			e.printStackTrace();
		} catch (JsonMappingException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Deserializes a transformer rule from a JSON string.
	 * 
	 * @param jsonString the JSON string to deserialize
	 * @return the deserialized transformer rule, or null if deserialization fails
	 */
	public ITransformerRule deserializeTransformerFromJsonString(String jsonString) {

		try {
			return mapper.readValue(jsonString, AbstractTransformerRule.class);
		} catch (JsonParseException e) {

			e.printStackTrace();
		} catch (JsonMappingException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

		return null;
	}

}
