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
import java.util.List;
import jakarta.persistence.AttributeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ListAttributeConverter implements AttributeConverter< List<String>, String > {

    private static Logger logger = LogManager.getLogger(ListAttributeConverter.class);

 
    static ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public String convertToDatabaseColumn(List<String> data) {
 
        String dataJson = null;
        try {
            dataJson = objectMapper.writeValueAsString(data);
        } catch (final JsonProcessingException e) {
            logger.error("JSON writing error", e);
        }
 
        return dataJson;
    }
 
    @Override
    public List<String> convertToEntityAttribute(String dataJSON) {
 
    	if ( dataJSON == null )
    		dataJSON = "[]";
    	
        List<String> data = null;
        try {
            data =  objectMapper.readValue(dataJSON, List.class);
        } catch (final IOException e) {
            logger.error("\n\n\nJSON reading error", e);
        }
 
        return data;
    }

    
 
}