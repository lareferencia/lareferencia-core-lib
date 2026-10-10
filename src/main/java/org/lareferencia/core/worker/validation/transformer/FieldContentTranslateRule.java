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

package org.lareferencia.core.worker.validation.transformer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.Translation;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.w3c.dom.Node;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transformation rule that translates field content values based on a mapping
 * table.
 * <p>
 * This rule reads values from a test field and writes translated values to a
 * target field
 * (which can be the same field). It uses a configurable translation map that
 * can support
 * case-sensitive or case-insensitive matching, and can handle prefix-based
 * translations.
 * <p>
 * Common use cases include:
 * </p>
 * <ul>
 * <li>Vocabulary normalization (e.g., mapping variant subject terms to standard
 * ones)</li>
 * <li>Language code translation</li>
 * <li>Resource type standardization</li>
 * <li>License URI normalization</li>
 * </ul>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 * @see Translation
 */
@ValidatorRuleMeta
public class FieldContentTranslateRule extends AbstractTransformerRule {

	private static Logger logger = LogManager.getLogger(FieldContentTranslateRule.class);

	@Getter
	@JsonIgnore
	Map<String, String> translationMap;

	@Getter
	@SchemaProperty(order = 5)
	List<Translation> translationArray;

	@Setter
	@Getter
	@SchemaProperty(order = 1)
	String testFieldName;

	@Setter
	@Getter
	@SchemaProperty(order = 2)
	String writeFieldName;

	@Setter
	@Getter
	@SchemaProperty(defaultValue = "true", order = 3)
	Boolean replaceOccurrence = true;

	@Setter
	@Getter
	@SchemaProperty(defaultValue = "false", order = 4)
	Boolean testValueAsPrefix = false;


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
	 * Transforms the record by translating field values according to the
	 * translation map.
	 * 
	 * @param record   the OAI record to transform
	 * @param metadata the metadata to transform
	 * @return true if any value was transformed, false otherwise
	 */
	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

        boolean wasTransformed = false;
        // Track the destination, including when the source is a different field.
        // Counts handle repeated source values and mappings between existing terms.
        Map<String, Integer> destinationCounts = new HashMap<>();
        Set<Node> destinationNodes = new HashSet<>(metadata.getFieldNodes(writeFieldName));
        for (Node node : destinationNodes) {
            if (node.getFirstChild() != null && node.getFirstChild().getNodeValue() != null) {
                destinationCounts.merge(node.getFirstChild().getNodeValue(), 1, Integer::sum);
            }
        }

        for (Node node : metadata.getFieldNodes(testFieldName)) {
            if (node.getFirstChild() == null) continue;
            String occurrence = node.getFirstChild().getNodeValue();
            if (occurrence == null) continue;
            String translated = null;
            if (!Boolean.TRUE.equals(testValueAsPrefix)) {
                translated = translationMap.get(occurrence);
            } else {
                // Preserve dictionary ordering, but apply only the first matching prefix.
                for (String prefix : translationMap.keySet()) {
                    if (occurrence.startsWith(prefix)) {
                        translated = translationMap.get(prefix);
                        break;
                    }
                }
            }
            if (translated == null) continue;
            boolean sourceInDestination = destinationNodes.contains(node);
            // An identity translation already in the destination needs no mutation.
            if (sourceInDestination && occurrence.equals(translated)) continue;

            if (Boolean.TRUE.equals(replaceOccurrence)) {
                metadata.removeNode(node);
                wasTransformed = true;
                if (sourceInDestination) {
                    destinationCounts.computeIfPresent(occurrence, (value, count) -> count - 1);
                }
            }
            if (destinationCounts.getOrDefault(translated, 0) == 0) {
                metadata.addFieldOcurrence(writeFieldName, translated);
                destinationCounts.merge(translated, 1, Integer::sum);
                wasTransformed = true;
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
