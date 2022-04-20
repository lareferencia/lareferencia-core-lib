
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

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

public class RegexTranslateRule extends AbstractTransformerRule {

	@Setter
	@Getter
	String sourceFieldName;

	@Setter
	@Getter
	String targetFieldName;
	
	@Getter
	String regexSearch;

	@Setter
	@Getter
	String regexReplace;
	
	@Setter
	@Getter
	Boolean removeMatchingOccurrences = false;
	
	//private Predicate<String> regexPredicate;

	public RegexTranslateRule() {
		
		
	}
	
	public void setRegexSearch(String regexPattern) {
		this.regexSearch = regexPattern;
		//regexPredicate = Pattern.compile(regexPattern).asPredicate();
	}

	
	Set<String> existingValues = new HashSet<String>();
	String occr = null;
	String replace = null;

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {
		
		
		// setup existing values
		existingValues.clear();	
		for (Node node : metadata.getFieldNodes(sourceFieldName)) 
			existingValues.add( node.getFirstChild().getNodeValue() );
		

		boolean wasTransformed = false;

		// ciclo de reemplazo
		// recorre las ocurrencias del campo de nombre source y si la occrs matchea con el la expresion realiza los reemplazos 
		// aplicando el pattern
		for ( Node node : metadata.getFieldNodes(sourceFieldName) ) {
			
			occr = node.getFirstChild().getNodeValue();
			
			//if ( regexPredicate.test(occr) ) {
				
				replace = occr.replaceFirst(regexSearch, regexReplace);
	
				// Agrega instancia target con el contenido a reemplazar
				if ( !existingValues.contains(replace) ) {
					
					// Si esta marcada la opción remueve ocurrencia source
					if ( removeMatchingOccurrences )
						metadata.removeNode( node );
		
					metadata.addFieldOcurrence(this.getTargetFieldName(), replace );
					existingValues.add(replace);
					
					// si entra al ciclo al menos una vez entonces transformó
					wasTransformed = true;
				}
			
			//}
		}
		

		return wasTransformed;
	}

}
