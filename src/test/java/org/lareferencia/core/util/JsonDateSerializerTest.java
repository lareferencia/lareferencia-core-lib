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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("JsonDateSerializer Tests")
class JsonDateSerializerTest {

    @Test
    @DisplayName("Should serialize LocalDateTime to dd/MM/yyyy HH:mm:ss format")
    void testSerializeDateTime() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 10, 27, 14, 30, 45);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-10-27 14:30:45\"", json);
    }

    @Test
    @DisplayName("Should serialize with leading zeros for single digit day")
    void testSerializeSingleDigitDay() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 10, 5, 14, 30, 45);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-10-05 14:30:45\"", json);
    }

    @Test
    @DisplayName("Should serialize with leading zeros for single digit month")
    void testSerializeSingleDigitMonth() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 3, 27, 14, 30, 45);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-03-27 14:30:45\"", json);
    }

    @Test
    @DisplayName("Should serialize midnight time")
    void testSerializeMidnight() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 10, 27, 0, 0, 0);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-10-27 00:00:00\"", json);
    }

    @Test
    @DisplayName("Should serialize end of day time")
    void testSerializeEndOfDay() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 10, 27, 23, 59, 59);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-10-27 23:59:59\"", json);
    }

    @Test
    @DisplayName("Should serialize leap year date")
    void testSerializeLeapYear() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2024, 2, 29, 12, 0, 0);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2024-02-29 12:00:00\"", json);
    }

    @Test
    @DisplayName("Should serialize first day of year")
    void testSerializeFirstDayOfYear() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 1, 1, 0, 0, 1);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-01-01 00:00:01\"", json);
    }

    @Test
    @DisplayName("Should serialize last day of year")
    void testSerializeLastDayOfYear() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2025, 12, 31, 23, 59, 58);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2025-12-31 23:59:58\"", json);
    }

    @Test
    @DisplayName("Should handle year 2000 correctly")
    void testSerializeYear2000() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2000, 1, 1, 12, 0, 0);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"2000-01-01 12:00:00\"", json);
    }

    @Test
    @DisplayName("Should handle year 1999 correctly")
    void testSerializeYear1999() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(1999, 12, 31, 23, 59, 59);
        
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        SerializerProvider provider = mapper.getSerializerProvider();
        
        JsonDateSerializer serializer = new JsonDateSerializer();
        serializer.serialize(dateTime, generator, provider);
        generator.flush();
        
        String json = writer.toString();
        assertEquals("\"1999-12-31 23:59:59\"", json);
    }
}
