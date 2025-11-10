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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.IOAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.lareferencia.core.validation.Translation;
import org.w3c.dom.Node;

import java.io.*;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transformation rule that translates field content values based on a mapping table.
 * <p>
 * This rule reads values from a test field and writes translated values to a target field
 * (which can be the same field). It uses a configurable translation map that can support
 * case-sensitive or case-insensitive matching, and can handle prefix-based translations.
 * <p>
 * Common use cases include:
 * </p>
 * <ul>
 *   <li>Vocabulary normalization (e.g., mapping variant subject terms to standard ones)</li>
 *   <li>Language code translation</li>
 *   <li>Resource type standardization</li>
 *   <li>License URI normalization</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 * @see Translation
 */
public class FieldContentTranslateRule extends AbstractTransformerRule {

	private static Logger logger = LogManager.getLogger(FieldContentTranslateRule.class);

	@Getter
	@JsonIgnore
	Map<String, String> translationMap;

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
	
	Set<String> existingValues = new HashSet<String>();	

	/**
	 * Creates a new field content translation rule.
	 */
	public FieldContentTranslateRule() {
		this.translationMap = new TreeMap<String, String>(CaseInsensitiveComparator.INSTANCE);
	}

	/**
	 * Sets the translation array and populates the translation map.
	 * 
	 * @param list the list of translations
	 */
	public void setTranslationArray(List<Translation> list) {
		this.translationArray = list;

		for (Translation t : list) {
			this.translationMap.put(t.getSearch(), t.getReplace());
		}

		logger.debug(list);
	}

	/**
	 * Sets the translation map from a file.
	 * 
	 * @param filename the path to the translation file
	 */
	public void setTranslationMapFileName(String filename) {

		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF8"));

			String line = br.readLine();

			int lineNumber = 1;
			while (line != null) {

				String[] parsedLine = line.split("\\t");

				if (parsedLine.length != 2)
					throw new Exception("Formato de archivo " + filename + " incorrecto!! linea: " + lineNumber);

				this.translationMap.put(parsedLine[0], parsedLine[1]);

				logger.debug("cargado: " + line);
				line = br.readLine();
				lineNumber++;
			}

			br.close();

		} catch (FileNotFoundException e) {
			logger.error("!!!!!! No se encontró el archivo de valores controlados:" + filename);
		} catch (IOException e) {
			logger.error("!!!!!! No se encontró el archivo de valores controlados:" + filename);
		} catch (Exception e) {
			logger.error("!!!!!! No se encontró el archivo de valores controlados:" + filename);
		}

	}
	
	/**
	 * Transforms the record by translating field values according to the translation map.
	 * 
	 * @param record the OAI record to transform
	 * @param metadata the metadata to transform
	 * @return true if any value was transformed, false otherwise
	 */
	@Override
	public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) {

		boolean wasTransformed = false;
	
		// setup existing values
		existingValues.clear();	
		for (Node node : metadata.getFieldNodes(testFieldName)) 
			existingValues.add( node.getFirstChild().getNodeValue() );
		
		
		String occr = null;
		String translatedOccr = null;

		// recorre las ocurrencias del campo de test
		for ( Node node : metadata.getFieldNodes(testFieldName) ) {
		
			occr = node.getFirstChild().getNodeValue();
			
				// Busca el valor completo, no el prefijo
			if (!testValueAsPrefix) {

				// if translation contains the value and the translated value does not yet exists
				if (translationMap.containsKey(occr) && !existingValues.contains( translationMap.get(occr) )) {
					translatedOccr = translationMap.get(occr);
					wasTransformed |= !occr.equals(translatedOccr); 	
					
					if (replaceOccurrence) 
						metadata.removeNode(node);
					
					metadata.addFieldOcurrence(writeFieldName, translatedOccr);
					existingValues.add(translatedOccr);
					
				}

			} else { // Busca el prefijo

				Boolean found = false;
				// recorre los valores del diccionarrio de reemplazo
				for (String testValue : translationMap.keySet()) {

					// si el valor del diccionario de reemplazo es prefijo de la
					if (!found && occr.startsWith(testValue)) {
						translatedOccr = translationMap.get(testValue);
						wasTransformed = true;
						
						if (replaceOccurrence) 
							metadata.removeNode(node);
						
						metadata.addFieldOcurrence(writeFieldName, translatedOccr);
						existingValues.add(translatedOccr);

					}
				}
			}
			
			
			
		}

		return wasTransformed;
	}

	/**
	 * Sets the translation map for field value translations.
	 * 
	 * @param translationMap the map of source to target values
	 */
	public void setTranslationMap(Map<String, String> translationMap) {
		this.translationMap = new TreeMap<String, String>(CaseInsensitiveComparator.INSTANCE);
		this.translationMap.putAll(translationMap);
	}

	/**
	 * Case-insensitive string comparator for translation keys.
	 */
	static class CaseInsensitiveComparator implements Comparator<String> {
		public static final CaseInsensitiveComparator INSTANCE = new CaseInsensitiveComparator();

		public int compare(String first, String second) {
			// some null checks
			return first.compareToIgnoreCase(second);
		}
	}

}
