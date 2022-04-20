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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

public class RemoveDuplicateVocabularyOccrsRule extends AbstractTransformerRule {
	
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;

	@Setter
	@Getter
	@JsonProperty("vocabulary")
	protected List<String> vocabulary;

	public RemoveDuplicateVocabularyOccrsRule() {
		vocabulary = new ArrayList<String>();
	}
	
	List<Pair<Integer, Node>> vocabularyOccurreces = new ArrayList< Pair<Integer,Node> >();

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {
		
		vocabularyOccurreces.clear();

		boolean wasTransformed = false;
				
		// recorre las ocurrencias del campo de test
		for ( Node node : metadata.getFieldNodes(fieldName) ) {

			String occr = node.getFirstChild().getNodeValue();

			// Si la ocurrencia está en el vocabulario se obtiene el numero de orden
			int foundAtindex = vocabulary.indexOf(occr);
			
			if ( foundAtindex != -1 ) // se crea una entrada con el numero de orden y el nodo
				vocabularyOccurreces.add( new ImmutablePair<Integer,Node>(foundAtindex, node) );
				
		}
			
		if ( vocabularyOccurreces.size() > 1 ) { // solo si hay repeticiones
			
			// ordena la lista 
			vocabularyOccurreces.sort( Comparator.comparing(Pair::getLeft) );
			vocabularyOccurreces.remove(0); // quita la primera aparitción para asegurar que la ocurrencia con mas prioridad no sea borrada
			
			for (Pair<Integer, Node> vocOccr : vocabularyOccurreces) { // remueve el resto de las occurrencias
				metadata.removeNode( vocOccr.getRight() );
			}
			
			wasTransformed = true;
		}
			
		return wasTransformed;
	}

}
