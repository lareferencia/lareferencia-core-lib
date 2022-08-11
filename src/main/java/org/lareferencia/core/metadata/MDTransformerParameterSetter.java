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

package org.lareferencia.core.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MDTransformerParameterSetter {

	public static void setParametersFromObject(IMDFormatTransformer transformer, String parameterNamePrefix, Object obj) {

		if ( obj != null && parameterNamePrefix != null ) {
		
			Class<?> objClass = obj.getClass();
			Method[] methods = objClass.getDeclaredMethods();
	
	
			// for each field in class obtained by reflection
			for(Method method:methods) { 
	
				// if field is a public instance field
				if ( !Modifier.isStatic(method.getModifiers()) && method.getName().startsWith("get")  ) {
	
					try {
	
						// obtain the value of the field for object obj
						Object valueOfResult = method.invoke(obj);
	
						if ( valueOfResult != null ) { // if value is not null
							// set the parameter using the prefixed name of field and the string value of the field 
							String fieldName = parameterNamePrefix + method.getName().substring(3).toLowerCase(); 
							if (valueOfResult instanceof String[]) {
								List<String> items = Arrays.asList((String[])valueOfResult);
								transformer.setParameter(fieldName, items);
							} else {
								transformer.setParameter(fieldName, valueOfResult.toString() );
							}
						}
	
					} catch (IllegalArgumentException | IllegalAccessException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} 
		}
	}
	
	public static void setParametersFromMap(IMDFormatTransformer transformer, String parameterNamePrefix, Map<String,Object> map) {

		if ( map != null && parameterNamePrefix != null ) {
		
			// for each field in map
			for(String name: map.keySet()) { 
	
				// obtain the value of the object
				Object valueOfResult = map.get(name);
	
					if ( valueOfResult != null ) { // if value is not null
						// set the parameter using the prefixed name of field and the string value of the field 
						String fieldName = parameterNamePrefix + name.toLowerCase(); 
						if (valueOfResult instanceof List) {
							List<String> items = (List<String>) valueOfResult; 
							transformer.setParameter(fieldName, items);
						} else {
							transformer.setParameter(fieldName, valueOfResult.toString() );
						}
					}
				}
			} 
	}
}
