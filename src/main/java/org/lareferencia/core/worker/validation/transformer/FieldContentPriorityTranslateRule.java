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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import lombok.Getter;
import lombok.Setter;

/**
 * Transformation rule that translates field values based on priority-ordered
 * translations.
 * <p>
 * Matches field values against a prioritized list of translations and replaces
 * them with corresponding target values. Supports prefix matching and
 * occurrence replacement.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
@ValidatorRuleMeta(name = "Traducción de valores de campos con orden de prioridad", help = "Matches field values against a prioritized list of translations and replaces them with corresponding target values.")
public class FieldContentPriorityTranslateRule extends AbstractTransformerRule {

	private static Logger logger = LogManager.getLogger(FieldContentPriorityTranslateRule.class);

	@Getter
	@SchemaProperty(title = "Listado de traducciones", description = "Si se encuentra una ocurrencia con alguno de los valores listado se reemplaza.", order = 5)
	List<Translation> translationArray;

	@Setter
	@Getter
	@SchemaProperty(title = "Campo de búsqueda", description = "El nombre del campo oai_dc donde se buscara el valor. Ej: dc.type", order = 1)
	String testFieldName;

	@Setter
	@Getter
	@SchemaProperty(title = "Campo de escritura", description = "El nombre del campo oai_dc que se creará con la ocurrencia de reemplazo Ej: dc.type", order = 2)
	String writeFieldName;

	@Setter
	@Getter
	@SchemaProperty(title = "¿Se reemplaza la ocurrencia encontrada?", description = "Indica si se eliminará la ocurrencia en el campo de búsqueda", defaultValue = "true", order = 3)
	Boolean replaceOccurrence = true;

	@Setter
	@Getter
	@SchemaProperty(title = "¿Evaluar como prefijo?", description = "Indica si el valor de búsqueda se evaluará como prefijo del contenido del campo de búsqueda.", defaultValue = "false", order = 4)
	Boolean testValueAsPrefix = false;

	@Setter
	@Getter
	@SchemaProperty(title = "¿Reemplazar todas las ocurrencias?", description = "Indica si se deben reemplazar todas las ocurrencias coincidentes o solo la primera.", defaultValue = "true", order = 6)
	Boolean replaceAllMatchingOccurrences = true;

	/**
	 * Creates a new priority translation rule.
	 */
	public FieldContentPriorityTranslateRule() {
	}

	/**
	 * Sets the translation array with priority order.
	 * 
	 * @param list the list of translations
	 */
	public void setTranslationArray(List<Translation> list) {
		this.translationArray = list;
		logger.debug(list);
	}

	Set<String> existingValues = new HashSet<String>();

	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		// setup existing values
		existingValues.clear();
		for (Node node : metadata.getFieldNodes(testFieldName))
			existingValues.add(node.getFirstChild().getNodeValue());

		String occr = null;

		boolean matchFound = false;

		for (Translation trl : this.getTranslationArray()) {

			// recorre las ocurrencias del campo de test
			for (Node node : metadata.getFieldNodes(testFieldName)) {

				occr = node.getFirstChild().getNodeValue();

				if (testValueAsPrefix) // si se debe testear el prefijo
					matchFound = occr.startsWith(trl.getSearch());
				else // caso de testing de valor completo
					matchFound = occr.equals(trl.getSearch());

				// if found and not already exists
				if (matchFound && !existingValues.contains(trl.getReplace())) {

					if (replaceOccurrence) // Si esta marcao el reemplazo del
						metadata.removeNode(node); // remueve la ocurrencia

					// agrega la ocurrencia
					metadata.addFieldOcurrence(writeFieldName, trl.getReplace());
					existingValues.add(trl.getReplace());

				}

				// si no deben reemplazare todas las ocurrencias y fue encontrada
				if (!replaceAllMatchingOccurrences && matchFound)
					break;
			}

			// si no deben reemplazare todas las ocurrencias y fue encontrada
			if (!replaceAllMatchingOccurrences && matchFound)
				break;
		}

		return matchFound;
	}

}