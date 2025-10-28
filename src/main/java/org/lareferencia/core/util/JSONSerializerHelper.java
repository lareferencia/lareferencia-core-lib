
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lareferencia.backend.domain.Validator;

import java.io.File;
import java.io.IOException;

/**
 * Helper class for JSON serialization and deserialization operations.
 */
public class JSONSerializerHelper {

	/**
	 * Private constructor to prevent instantiation of utility class.
	 */
	private JSONSerializerHelper() {
		throw new UnsupportedOperationException("Utility class");
	}

	/**
	 * Serializes an object to a JSON string.
	 *
	 * @param obj the object to serialize
	 * @return JSON string representation
	 * @throws JsonProcessingException if serialization fails
	 */
    public static String serializeToJsonString(Object obj) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(obj);

    }

	/**
	 * Deserializes a JSON string to an object of the specified class.
	 *
	 * @param jsonString the JSON string
	 * @param retClass the target class
	 * @return deserialized object
	 * @throws JsonProcessingException if deserialization fails
	 */
    public static Object deserializeFromJsonString(String jsonString, Class retClass) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, retClass);
    }

	/**
	 * Deserializes a JSON file to an object of the specified class.
	 *
	 * @param file the JSON file
	 * @param retClass the target class
	 * @return deserialized object
	 * @throws IOException if file reading or deserialization fails
	 */
    public static Object deserializeFromFile(File file, Class retClass) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(file, retClass);
    }



}
