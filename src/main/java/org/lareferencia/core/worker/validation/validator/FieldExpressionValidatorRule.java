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

package org.lareferencia.core.worker.validation.validator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractValidatorRule;
import org.lareferencia.core.worker.validation.ValidatorRuleResult;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

import lombok.Getter;
import lombok.Setter;

/**
 * La clase abstracta BaseContentVRule implementa los mecanismos comunes para la
 * evaluación de contenidos de distintas ocurrencias de un mismo metadato.
 * 
 * @author lmatas
 * 
 */

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.PROPERTY, property = "@class")
public class FieldExpressionValidatorRule extends AbstractValidatorRule {
	
	private static Logger logger = LogManager.getLogger(FieldExpressionValidatorRule.class);

	@JsonProperty("expression")
	private String expression;

	/**
	 * Evaluator for processing field expressions.
	 */
	FieldExpressionEvaluator evaluator;

	/**
	 * Creates a new field expression validator rule.
	 */
	public FieldExpressionValidatorRule() {
		evaluator = new FieldExpressionEvaluator(this.quantifier);
	}

	/**
	 * Validates the metadata against the configured expression.
	 * 
	 * @param metadata the metadata to validate
	 * @return the validation result
	 */
	public ValidatorRuleResult validate(OAIRecordMetadata metadata) {

		ValidatorRuleResult result = new ValidatorRuleResult();
		
		boolean isRuleValid = false;
		
		try {
			isRuleValid = evaluator.evaluate(expression, metadata);
		} catch (Exception | StackOverflowError e) {
			logger.error( e + " oai_identifier:" + metadata.getIdentifier() + " msg:: " + e.getMessage() + "  regexp= " + expression ); 
		}
		
		result.setRule(this);
		result.setResults(evaluator.getEvaluationResults());
		result.setValid(isRuleValid);
		return result;

	}

}
