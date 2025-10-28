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

import java.io.IOException;
import java.util.Map;

import jakarta.persistence.AttributeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * JPA attribute converter for storing Map objects as JSON in database.
 */
public class MapAttributeConverter implements AttributeConverter< Map<String,Object>, String > {

    private static Logger logger = LogManager.getLogger(MapAttributeConverter.class);

 
    static ObjectMapper objectMapper = new ObjectMapper();
    
	/**
	 * Constructs a new map attribute converter.
	 */
	public MapAttributeConverter() {
		// Default constructor
	}

    @Override
    public String convertToDatabaseColumn(Map<String, Object> data) {
 
        String dataJson = null;
        try {
            dataJson = objectMapper.writeValueAsString(data);
        } catch (final JsonProcessingException e) {
            logger.error("JSON writing error", e);
        }
 
        return dataJson;
    }
 
    @Override
    public Map<String, Object> convertToEntityAttribute(String dataJSON) {
 
    	if ( dataJSON == null )
    		dataJSON = "{}";
    	
        Map<String, Object> data = null;
        try {
            data =  objectMapper.readValue(dataJSON, Map.class);
        } catch (final IOException e) {
            logger.error("\n\n\nJSON reading error", e);
        }
 
        return data;
    }

    
 
}