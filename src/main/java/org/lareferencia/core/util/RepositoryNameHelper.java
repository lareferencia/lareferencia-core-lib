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

package org.lareferencia.core.util;

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.springframework.stereotype.Component;
import org.w3c.dom.Node;

/**
 * Utility class for managing repository names and metadata field operations.
 * Provides methods for appending and managing repository-related information in OAI metadata.
 */
@Component
public class RepositoryNameHelper {

	
	
	/**
	 * Constructs a new RepositoryNameHelper instance.
	 */
	public RepositoryNameHelper() {
		
		/*
		try {
			pattern = Pattern.compile(DOMAIN_NAME_PATTERN_STR);
		} catch (PatternSyntaxException e) {
			logger.error("RepositoryNameHelper::Error en el patron: " +DOMAIN_NAME_PATTERN_STR);

		}*/

	}
/*
	public void setDetectREPattern(String patternString) {

		try {
			pattern = Pattern.compile(patternString);
		} catch (PatternSyntaxException e) {
			logger.error("RepositoryNameHelper::Error en el patron: " +patternString);

		}
	}


	public static final String DOMAIN_NAME_PATTERN_STR = "[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z-]{2,})";
	private static final String NAME_PATTERN_STR = "[A-Za-z0-9-]{4,}";

	public String detectRepositoryDomain(String identifier) {

		String result = UNKNOWN;

		Matcher matcher = pattern.matcher(identifier);

		if (matcher.find())
			result = matcher.group();

		return result;
	}
*/
	/**
	 * Default name for repositories that cannot be classified.
	 */
	public static String UNKNOWN = "No clasificados";
	
	
	

	/**
	 * Appends a name value to a metadata field with a specified prefix.
	 * Optionally replaces an existing field occurrence with the same prefix.
	 * 
	 * @param metadata the OAI record metadata to modify
	 * @param fieldname the name of the field to append to
	 * @param prefix the prefix to use for the value
	 * @param value the value to append
	 * @param replaceExisting true to replace existing occurrences with the same prefix, false to keep them
	 */
	public void appendNameToMetadata(OAIRecordMetadata metadata, String fieldname, String prefix, String value, Boolean replaceExisting) {

		Node existingNode = null;

		for (Node node : metadata.getFieldNodes(fieldname)) {

			String occr = node.getFirstChild().getNodeValue();

			if (occr.startsWith(prefix))
				existingNode = node;
		}

		if (existingNode != null) {
			if (replaceExisting) {
				Node fieldNode = existingNode.getParentNode();
				fieldNode.removeChild(existingNode);
				metadata.addFieldOcurrence(fieldname, prefix + value);
			}

		} else {
			metadata.addFieldOcurrence(fieldname, prefix + value);
		}
	}
	
	/**
	 * Removes duplicate field occurrences with a specified prefix from the metadata.
	 * 
	 * @param metadata the OAI record metadata to modify
	 * @param fieldname the name of the field to check for duplicates
	 * @param prefix the prefix to match for removal
	 * @return true if duplicates were found and removed, false otherwise
	 */
	public static boolean removeDuplicates(OAIRecordMetadata metadata, String fieldname, String prefix) {
		
		List<Node> matchingNodeList = new ArrayList<Node>();
		
		for (Node node : metadata.getFieldNodes(fieldname)) {

			String occr = node.getFirstChild().getNodeValue();

			if ( occr.startsWith(prefix) )
				matchingNodeList.add(node);
		}
		
		if ( matchingNodeList.size() > 1 ) {
			matchingNodeList.remove(0);
			
			for (Node node : matchingNodeList) {
				metadata.removeNode(node);
			}
			
			return true;
		} else 
			return false;

	}

}
