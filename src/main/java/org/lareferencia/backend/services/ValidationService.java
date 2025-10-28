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

package org.lareferencia.backend.services;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Transformer;
import org.lareferencia.backend.domain.TransformerRule;
import org.lareferencia.backend.domain.Validator;
import org.lareferencia.backend.domain.ValidatorRule;
import org.lareferencia.backend.workers.validator.TransformerImpl;
import org.lareferencia.backend.workers.validator.ValidatorImpl;
import org.lareferencia.core.validation.ITransformer;
import org.lareferencia.core.validation.ITransformerRule;
import org.lareferencia.core.validation.IValidator;
import org.lareferencia.core.validation.IValidatorRule;
import org.lareferencia.core.validation.RuleSerializer;
import org.lareferencia.core.validation.ValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Service that manages the creation and serialization of validators and transformers.
 * <p>
 * This service acts as a bridge between the domain model (persistent entities) and
 * the runtime validation/transformation objects. It uses {@link RuleSerializer} to
 * convert between JSON representations and executable rule instances.
 * <p>
 * Key responsibilities include:
 * </p>
 * <ul>
 *   <li>Creating {@link IValidator} instances from {@link Validator} models</li>
 *   <li>Creating {@link ITransformer} instances from {@link Transformer} models</li>
 *   <li>Persisting runtime validators back to domain models</li>
 *   <li>Managing rule ordering and execution sequence</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see Validator
 * @see Transformer
 * @see RuleSerializer
 */
@Component
public class ValidationService {
	
	private static Logger logger = LogManager.getLogger(ValidationService.class);

	/**
	 * Constructs a new validation service.
	 */
	public ValidationService() {
		// Default constructor for Spring
	}

	@Autowired
	private RuleSerializer serializer;
	
	
	private Ordering<TransformerRule> ruleByOrderOrdering = new Ordering<TransformerRule>() {
		  public int compare(TransformerRule left, TransformerRule right) {
		    return Ints.compare(left.getRunorder(), right.getRunorder());
		  }
	};
	

	/**
	 * Creates a runtime validator instance from a domain validator model.
	 * <p>
	 * Deserializes all validation rules from their JSON representations and
	 * configures them with properties from the model (mandatory, quantifier).
	 * </p>
	 * 
	 * @param vmodel the validator domain model containing rule definitions
	 * @return a configured IValidator instance ready for use
	 * @throws ValidationException if any rule cannot be deserialized or configured
	 */
	public IValidator createValidatorFromModel(Validator vmodel) throws ValidationException {
		


		IValidator validator = new ValidatorImpl();

		for (ValidatorRule vrule : vmodel.getRules()) {
			
			try {

				IValidatorRule rule = serializer.deserializeValidatorFromJsonString(vrule.getJsonserialization());
	
				/* Estas propiedades son cargadas desde el modelo en el objeto rule */
				/*
				 * name y description no existen en el modelo de objetos interno
				 * porque no resultan útiles al procesamiento
				 */
				rule.setRuleId(vrule.getId());
				rule.setMandatory(vrule.getMandatory());
				rule.setQuantifier(vrule.getQuantifier());
	
				validator.getRules().add(rule);
			}
			catch (Exception e) {
				throw new ValidationException("Error loading validation rule: " + vrule.getId() + "::" + vrule.getName() + " :: " + e.getMessage(), e);
			}

		}

		return validator;
	}

	/**
	 * Creates a runtime transformer instance from a domain transformer model.
	 * <p>
	 * Deserializes all transformation rules from their JSON representations
	 * and ensures they are executed in the correct order based on the runorder field.
	 * Rules are sorted before being added to the transformer to guarantee proper
	 * execution sequence.
	 * </p>
	 * 
	 * @param tmodel the transformer domain model containing rule definitions
	 * @return a configured ITransformer instance with ordered rules
	 * @throws ValidationException if any rule cannot be deserialized or configured
	 */
	public ITransformer createTransformerFromModel(Transformer tmodel) throws ValidationException {

		ITransformer transformer = new TransformerImpl();
		
		//logger.debug( "UNSORTED:" + tmodel.getRules() );
		
		// se ordena la lista en base al atributo order para generar el transformador con reglas ordenadas
		List<TransformerRule> trules = ruleByOrderOrdering.sortedCopy(tmodel.getRules() );
		
		//logger.debug( "SORTED:" + trules );

		
		for (TransformerRule trule : trules) {
			
			try {
				ITransformerRule rule = serializer.deserializeTransformerFromJsonString(trule.getJsonserialization());
				rule.setRuleId(trule.getId());
				transformer.getRules().add(rule);			
			}
			catch (Exception e) {
				throw new ValidationException("Error loading transformation rule: " + trule.getId() + "::" + trule.getName() + " :: " + e.getMessage(), e);
			}
		}
		
		
		//logger.debug( "SortedFinalRules:" + transformer.getRules() );


		return transformer;
	}

	/**
	 * Creates a persistent validator model from a runtime validator instance.
	 * <p>
	 * Serializes all validation rules to JSON format and creates a domain model
	 * suitable for database persistence. This allows saving runtime-configured
	 * validators for later reuse.
	 * </p>
	 * 
	 * @param validator the runtime validator instance to persist
	 * @param name the name for the validator model
	 * @param description the description for the validator model
	 * @return a Validator domain model ready for persistence
	 */
	public Validator createModelFromValidator(IValidator validator, String name, String description) {

		Validator validatorModel = new Validator();

		for (IValidatorRule vrule : validator.getRules()) {

			ValidatorRule ruleModel = new ValidatorRule();

			ruleModel.setName(vrule.getRuleId().toString());
			ruleModel.setDescription("");
			ruleModel.setMandatory(vrule.getMandatory());
			ruleModel.setQuantifier(vrule.getQuantifier());
			ruleModel.setJsonserialization(serializer.serializeValidatorToJsonString(vrule));

			validatorModel.getRules().add(ruleModel);

		}

		validatorModel.setName(name);
		validatorModel.setDescription(description);

		return validatorModel;
	}

}
