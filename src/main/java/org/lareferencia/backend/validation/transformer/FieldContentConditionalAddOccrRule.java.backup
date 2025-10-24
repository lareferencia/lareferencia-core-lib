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

package org.lareferencia.backend.validation.transformer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.validation.validator.FieldExpressionEvaluator;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.lareferencia.core.validation.QuantifierValues;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FieldContentConditionalAddOccrRule extends AbstractTransformerRule {

	FieldExpressionEvaluator evaluator;
	
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	@Setter
	@Getter
	@JsonProperty("valueToAdd")
	String valueToAdd;
	
	@Setter
	@Getter
	@JsonProperty("conditionalExpression")
	private String conditionalExpression;
	
	@JsonIgnore
	protected QuantifierValues quantifier = QuantifierValues.ONE_OR_MORE;


	public FieldContentConditionalAddOccrRule() {
		evaluator = new FieldExpressionEvaluator(this.quantifier);
	}

	@Setter
	@Getter
	@JsonProperty("removeDuplicatedOccurrences")
	private Boolean removeDuplicatedOccurrences = false;



	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {

		boolean isExpressionValid = evaluator.evaluate(conditionalExpression, metadata);		
		boolean wasTransformed = false;
		
		
		if ( isExpressionValid ) {
			wasTransformed = true;
			metadata.addFieldOcurrence(fieldName, valueToAdd);
		}
		
		
		Set<String> occrSet  = new HashSet<String>();
		List<Node> removeList = new ArrayList<Node>();
		
		if ( removeDuplicatedOccurrences ) {

			// recorre las ocurrencias del campo de test
			for ( Node node : metadata.getFieldNodes(fieldName) ) {
	
				String occr = node.getFirstChild().getNodeValue();
	
				if ( occrSet.contains(occr) )
					removeList.add(node);
				else
					occrSet.add(occr);
			}
			
			for (Node node: removeList) 
				metadata.removeNode(node);
		}

		return wasTransformed;
	}

	

}
