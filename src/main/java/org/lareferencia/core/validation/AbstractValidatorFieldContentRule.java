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

package org.lareferencia.core.validation;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.OAIRecordMetadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

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
public abstract class AbstractValidatorFieldContentRule extends AbstractValidatorRule implements IValidatorFieldContentRule {

	@JsonProperty("fieldname")
	private String fieldname;

	public AbstractValidatorFieldContentRule() {
	}

	/**
	 * Esta función abstracta será implementada en las derivadas y determina la
	 * valides de un string
	 * 
	 * @param metadata
	 * @return
	 */
	public ValidatorRuleResult validate(OAIRecordMetadata metadata) {

		
		ValidatorRuleResult result = new ValidatorRuleResult();

		List<ContentValidatorResult> results = new ArrayList<ContentValidatorResult>();
		int validOccurrencesCount = 0;
		int occurrencesCount = 0;

		List<String> occurrences = metadata.getFieldOcurrences(fieldname);

		for (String fieldValue : occurrences) {

			// Se valida cada ocurrencia y se obtiene el resultado
			ContentValidatorResult occurrenceResult = this.validate(fieldValue);

			// Se agrega a la lista de ocurrencias
			results.add(occurrenceResult);
			
			// processed records invalid or valid ones
			occurrencesCount += 1;

			// Se suman las ocurrencias válidas
			validOccurrencesCount += occurrenceResult.isValid() ? 1 : 0;
		}
		
		// SI NO HAY OCCRS LO INDICA COMO UN VALOR DE RESULTADO
		if ( occurrences.size() == 0 ) {
			ContentValidatorResult occurrenceResult = new ContentValidatorResult();
			occurrenceResult.setReceivedValue("no_occurrences_found");
			occurrenceResult.setValid(false);
			results.add(occurrenceResult);
		}

		boolean isRuleValid;

		switch (quantifier) {

		case ONE_ONLY:
			isRuleValid = validOccurrencesCount == 1;
			break;

		case ONE_OR_MORE:
			isRuleValid = validOccurrencesCount >= 1;
			break;

		case ZERO_OR_MORE:
            // If we have at least one processed entry and if it's invalid
            if (occurrencesCount > 0 && validOccurrencesCount == 0) {
                isRuleValid = false;
            } else {
                isRuleValid = validOccurrencesCount >= 0;
            }
			break;

		case ZERO_ONLY:
			isRuleValid = validOccurrencesCount == 0;
			break;

		case ALL:
			isRuleValid = validOccurrencesCount == occurrences.size();
			break;

		default:
			isRuleValid = false;
			break;
		}

		result.setRule(this);
		result.setResults(results);
		result.setValid(isRuleValid);
		return result;

	}

	/**
	 * Esta función abstracta será implementada en las derivadas y determina la
	 * valides de un string
	 * 
	 * @param string el contenido a validar
	 * @return resultado de la validación
	 */
	public abstract ContentValidatorResult validate(String string);
}
