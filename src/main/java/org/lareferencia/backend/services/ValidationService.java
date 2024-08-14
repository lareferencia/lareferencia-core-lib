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
 * 
 * ValidatorManager toma como parámetro objetos del modelo, validator y
 * tranformer y usando RuleSerializer devuelve objetos validatores y
 * transformadores para se usandos en los procesos de validación y
 * transformación del worker
 * 
 * @author lmatas
 * 
 */
@Component
public class ValidationService {
	
	private static Logger logger = LogManager.getLogger(ValidationService.class);


	@Autowired
	private RuleSerializer serializer;
	
	
	private Ordering<TransformerRule> ruleByOrderOrdering = new Ordering<TransformerRule>() {
		  public int compare(TransformerRule left, TransformerRule right) {
		    return Ints.compare(left.getRunorder(), right.getRunorder());
		  }
	};
	

	/**
	 * Crea un validador a partir de un validador modelo
	 * 
	 * @param vmodel
	 * @return
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
	 * Crea un transformador a partir de un validador modelo
	 * 
	 * @param vmodel
	 * @return
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
	 * Crea un model de validador para persistir a partir de un objeto validador
	 * 
	 * @param validator
	 * @param name
	 * @param description
	 * @return
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
