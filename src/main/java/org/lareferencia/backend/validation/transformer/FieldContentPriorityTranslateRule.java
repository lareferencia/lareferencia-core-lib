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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.lareferencia.core.validation.Translation;
import org.w3c.dom.Node;

import lombok.Getter;
import lombok.Setter;

public class FieldContentPriorityTranslateRule extends AbstractTransformerRule {
	
	
	private static Logger logger = LogManager.getLogger(FieldContentPriorityTranslateRule.class);

	@Getter
	List<Translation> translationArray;

	@Setter
	@Getter
	String testFieldName;

	@Setter
	@Getter
	String writeFieldName;

	@Setter
	@Getter
	Boolean replaceOccurrence = true;

	@Setter
	@Getter
	Boolean testValueAsPrefix = false;
	
	@Setter
	@Getter
	Boolean replaceAllMatchingOccurrences = true;
	

	public FieldContentPriorityTranslateRule() {
	}

	public void setTranslationArray(List<Translation> list) {
		this.translationArray = list;
		logger.debug(list);
	}

	
	Set<String> existingValues = new HashSet<String>();	

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {
		
		
		// setup existing values
		existingValues.clear();	
		for (Node node : metadata.getFieldNodes(testFieldName)) 
			existingValues.add( node.getFirstChild().getNodeValue() );
		
		
		String occr = null;

		boolean matchFound = false;

		for ( Translation trl : this.getTranslationArray() ) {
			
			// recorre las ocurrencias del campo de test
			for ( Node node : metadata.getFieldNodes(testFieldName) ) {
				
				occr = node.getFirstChild().getNodeValue();
								
				if ( testValueAsPrefix )  // si se debe testear el prefijo
					matchFound = occr.startsWith(trl.getSearch());
				else // caso de testing de valor completo
					matchFound = occr.equals(trl.getSearch());
				
				// if found and not already exists
				if  ( matchFound && !existingValues.contains(trl.getReplace()) ) { 
					
					if (replaceOccurrence) // Si esta marcao el reemplazo del
						metadata.removeNode( node ); // remueve la ocurrencia
						
					// agrega la ocurrencia
					metadata.addFieldOcurrence(writeFieldName, trl.getReplace());
					existingValues.add(trl.getReplace());
					
				}
				
				// si no deben reemplazare todas las ocurrencias y fue encontrada
				if ( !replaceAllMatchingOccurrences && matchFound)
					break;
			}
			
			// si no deben reemplazare todas las ocurrencias y fue encontrada
			if ( !replaceAllMatchingOccurrences && matchFound)
				break;
		}
		
		return matchFound;
	}



	

}