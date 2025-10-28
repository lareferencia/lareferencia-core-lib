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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OAIMetadataBitstream
 */
@DisplayName("OAIMetadataBitstream Tests")
class OAIMetadataBitstreamTest {

    private OAIMetadataBitstream bitstream;

    @BeforeEach
    void setUp() {
        bitstream = new OAIMetadataBitstream();
    }

    @Test
    @DisplayName("Should create OAIMetadataBitstream with default constructor")
    void testDefaultConstructor() {
        assertNotNull(bitstream);
        assertNull(bitstream.getSid());
        assertNull(bitstream.getType());
        assertNull(bitstream.getName());
        assertNull(bitstream.getFormat());
        assertNull(bitstream.getSize());
        assertNull(bitstream.getUrl());
    }

    @Test
    @DisplayName("Should set and get sid")
    void testSidProperty() {
        Integer sid = 12345;
        bitstream.setSid(sid);
        
        assertEquals(sid, bitstream.getSid());
    }

    @Test
    @DisplayName("Should set and get type")
    void testTypeProperty() {
        String type = "application/pdf";
        bitstream.setType(type);
        
        assertEquals(type, bitstream.getType());
    }

    @Test
    @DisplayName("Should set and get name")
    void testNameProperty() {
        String name = "document.pdf";
        bitstream.setName(name);
        
        assertEquals(name, bitstream.getName());
    }

    @Test
    @DisplayName("Should set and get format")
    void testFormatProperty() {
        String format = "PDF";
        bitstream.setFormat(format);
        
        assertEquals(format, bitstream.getFormat());
    }

    @Test
    @DisplayName("Should set and get size")
    void testSizeProperty() {
        String size = "1024";
        bitstream.setSize(size);
        
        assertEquals(size, bitstream.getSize());
    }

    @Test
    @DisplayName("Should set and get url")
    void testUrlProperty() {
        String url = "http://example.com/document.pdf";
        bitstream.setUrl(url);
        
        assertEquals(url, bitstream.getUrl());
    }

    @Test
    @DisplayName("Should set and get checksum")
    void testChecksumProperty() {
        String checksum = "abc123def456";
        bitstream.setChecksum(checksum);
        
        assertEquals(checksum, bitstream.getChecksum());
    }

    @Test
    @DisplayName("Should return checksum when set")
    void testGetChecksumWhenSet() {
        String checksum = "test-checksum";
        bitstream.setChecksum(checksum);
        
        assertEquals(checksum, bitstream.getChecksum());
    }

        @Test
    @DisplayName("Should handle null checksum")
    void testNullChecksum() {
        OAIMetadataBitstream bitstream = new OAIMetadataBitstream();
        bitstream.setChecksum(null);
        // getChecksum() returns null when checksum is null or empty
        assertNull(bitstream.getChecksum());
    }

    @Test
    @DisplayName("Should handle empty checksum")
    void testEmptyChecksum() {
        bitstream.setChecksum("");
        String result = bitstream.getChecksum();
        
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should generate toString with name, type and url")
    void testToString() {
        bitstream.setName("test.pdf");
        bitstream.setType("application/pdf");
        bitstream.setUrl("http://example.com/test.pdf");
        
        String result = bitstream.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("test.pdf"));
        assertTrue(result.contains("application/pdf"));
        assertTrue(result.contains("http://example.com/test.pdf"));
    }

    @Test
    @DisplayName("Should handle null values in toString")
    void testToStringWithNulls() {
        String result = bitstream.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("null"));
    }

    @Test
    @DisplayName("Should handle zero sid")
    void testZeroSid() {
        bitstream.setSid(0);
        assertEquals(0, bitstream.getSid());
    }

    @Test
    @DisplayName("Should handle negative sid")
    void testNegativeSid() {
        bitstream.setSid(-1);
        assertEquals(-1, bitstream.getSid());
    }

    @Test
    @DisplayName("Should handle large sid values")
    void testLargeSid() {
        Integer largeSid = Integer.MAX_VALUE;
        bitstream.setSid(largeSid);
        
        assertEquals(largeSid, bitstream.getSid());
    }

    @Test
    @DisplayName("Should handle long file names")
    void testLongFileName() {
        String longName = "very_long_file_name_" + "a".repeat(200) + ".pdf";
        bitstream.setName(longName);
        
        assertEquals(longName, bitstream.getName());
    }

    @Test
    @DisplayName("Should handle special characters in name")
    void testSpecialCharactersInName() {
        String specialName = "file-name_with.special@chars#123.pdf";
        bitstream.setName(specialName);
        
        assertEquals(specialName, bitstream.getName());
    }

    @Test
    @DisplayName("Should handle Unicode in name")
    void testUnicodeInName() {
        String unicodeName = "archivo_español_ñáéíóú.pdf";
        bitstream.setName(unicodeName);
        
        assertEquals(unicodeName, bitstream.getName());
    }

    @Test
    @DisplayName("Should handle various URL formats")
    void testVariousUrlFormats() {
        String[] urls = {
            "http://example.com/file.pdf",
            "https://secure.example.com/file.pdf",
            "ftp://ftp.example.com/file.pdf",
            "/local/path/file.pdf",
            "file:///absolute/path/file.pdf"
        };
        
        for (String url : urls) {
            bitstream.setUrl(url);
            assertEquals(url, bitstream.getUrl());
        }
    }

    @Test
    @DisplayName("Should handle different format types")
    void testDifferentFormats() {
        String[] formats = {"PDF", "DOCX", "XLSX", "TXT", "HTML", "XML"};
        
        for (String format : formats) {
            bitstream.setFormat(format);
            assertEquals(format, bitstream.getFormat());
        }
    }

    @Test
    @DisplayName("Should set all properties together")
    void testAllProperties() {
        bitstream.setSid(123);
        bitstream.setType("application/pdf");
        bitstream.setName("document.pdf");
        bitstream.setFormat("PDF");
        bitstream.setSize("2048");
        bitstream.setUrl("http://example.com/document.pdf");
        bitstream.setChecksum("abc123");
        
        assertEquals(123, bitstream.getSid());
        assertEquals("application/pdf", bitstream.getType());
        assertEquals("document.pdf", bitstream.getName());
        assertEquals("PDF", bitstream.getFormat());
        assertEquals("2048", bitstream.getSize());
        assertEquals("http://example.com/document.pdf", bitstream.getUrl());
        assertEquals("abc123", bitstream.getChecksum());
    }
}
