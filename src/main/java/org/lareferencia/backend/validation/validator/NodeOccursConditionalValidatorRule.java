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

package org.lareferencia.backend.validation.validator;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractValidatorRule;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.w3c.dom.Node;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

import lombok.Getter;
import lombok.Setter;


/**
 * Validator rule that conditionally checks for the existence of nodes based on an XPath expression.
 * Validates that nodes matching the expression exist or don't exist depending on configuration.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.PROPERTY, property = "@class")
public class NodeOccursConditionalValidatorRule extends AbstractValidatorRule {
	
	private static Logger logger = LogManager.getLogger(NodeOccursConditionalValidatorRule.class);
	
	/**
	 * Constructs a new node occurs conditional validator rule.
	 */
	public NodeOccursConditionalValidatorRule() {
		super();
	}
	
	/**
	 * The XPath expression used to find nodes for validation.
	 */
    @Getter
	@JsonProperty("xpathExpression")
    String sourceXPathExpression;

	
	/**
	 * Sets the XPath expression to evaluate for node existence validation.
	 * 
	 * @param xpathExpression the XPath expression to use
	 */
    public void setSourceXPathExpression(String xpathExpression) {
        this.sourceXPathExpression = xpathExpression;
        //regexPredicate = Pattern.compile(regexPattern).asPredicate();
    }	

	/**
	 * Validates the metadata by checking for node occurrences matching the XPath expression.
	 * <p>
	 * Evaluates whether the number of matching nodes satisfies the configured quantifier
	 * (ONE_ONLY, ONE_OR_MORE, ZERO_OR_MORE, ZERO_ONLY, or ALL).
	 * </p>
	 * 
	 * @param metadata the record metadata to validate
	 * @return the validation result with occurrence details
	 */
	public ValidatorRuleResult validate(OAIRecordMetadata metadata) {
        ValidatorRuleResult result = new ValidatorRuleResult();

        List<ContentValidatorResult> results = new ArrayList<ContentValidatorResult>();
        int occurrencesCount = 0;

        boolean isRuleValid = false;

        List<Node> nodes = metadata.getFieldNodesByXPath(this.getSourceXPathExpression()); 
        occurrencesCount = nodes.size();
        
        for (Node node : nodes) {
            
            ContentValidatorResult occurrenceResult = new ContentValidatorResult();
            occurrenceResult.setReceivedValue(node.getNodeName());
            occurrenceResult.setValid(true);
            results.add(occurrenceResult);
        }
	    
        // SI NO HAY OCCRS LO INDICA COMO UN VALOR DE RESULTADO
        if ( occurrencesCount == 0 ) {
            ContentValidatorResult occurrenceResult = new ContentValidatorResult();
            occurrenceResult.setReceivedValue("no_occurrences_found");
            occurrenceResult.setValid(false);
            results.add(occurrenceResult);
        }
        

        switch (quantifier) {

        case ONE_ONLY:
            isRuleValid = occurrencesCount == 1;
            break;

        case ONE_OR_MORE:
            isRuleValid = occurrencesCount >= 1;
            break;

        case ZERO_OR_MORE:
            isRuleValid = occurrencesCount >= 0;
            break;

        case ZERO_ONLY:
            isRuleValid = occurrencesCount == 0;
            break;

        //TODO: verify if it makes sense - since the selector is the expression 
        case ALL:
            isRuleValid = occurrencesCount > 0;
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

}
