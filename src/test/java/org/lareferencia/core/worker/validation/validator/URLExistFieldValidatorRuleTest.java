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

package org.lareferencia.core.worker.validation.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("URLExistFieldValidatorRule Tests")
class URLExistFieldValidatorRuleTest {

    private URLExistFieldValidatorRule rule;

    @BeforeEach
    void setUp() {
        rule = new URLExistFieldValidatorRule();
    }

    @Test
    @DisplayName("Should handle null content")
    void testNullContent() {
        ContentValidatorResult result = rule.validate((String) null);

        assertFalse(result.isValid());
        assertEquals("NULL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle malformed URL")
    void testMalformedURL() {
        ContentValidatorResult result = rule.validate("not a valid url");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle malformed URL with invalid protocol")
    void testInvalidProtocol() {
        ContentValidatorResult result = rule.validate("htp://example.com");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle malformed URL with spaces")
    void testURLWithSpaces() {
        ContentValidatorResult result = rule.validate("http://example .com");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle unknown host")
    void testUnknownHost() {
        ContentValidatorResult result = rule.validate("http://thisdomaindoesnotexist12345.invalid");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle empty string")
    void testEmptyString() {
        ContentValidatorResult result = rule.validate("");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL without protocol")
    void testURLWithoutProtocol() {
        ContentValidatorResult result = rule.validate("www.example.com");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with only protocol")
    void testOnlyProtocol() {
        ContentValidatorResult result = rule.validate("http://");

        assertFalse(result.isValid());
        // Different JDKs/environments may return different errors
        assertNotNull(result.getReceivedValue());
        assertFalse(result.getReceivedValue().equals("OK"));
    }

    @Test
    @DisplayName("Should handle relative URL")
    void testRelativeURL() {
        ContentValidatorResult result = rule.validate("/path/to/resource");

        assertFalse(result.isValid());
        assertEquals("MalformedURL", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with special characters")
    void testURLWithSpecialCharacters() {
        ContentValidatorResult result = rule.validate("http://example.com/path?param=value&other=123");

        assertFalse(result.isValid());
        // Will likely fail due to connection error or unknown host (since it's a test)
        assertTrue(result.getReceivedValue().equals("UnknownHost") || 
                   result.getReceivedValue().equals("ConnectionError") ||
                   result.getReceivedValue().equals("ERROR"));
    }

    @Test
    @DisplayName("Should handle HTTPS URL")
    void testHTTPSURL() {
        ContentValidatorResult result = rule.validate("https://nonexistent-test-domain-12345.com");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with port")
    void testURLWithPort() {
        ContentValidatorResult result = rule.validate("http://nonexistent-domain-12345.com:8080");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with fragment")
    void testURLWithFragment() {
        ContentValidatorResult result = rule.validate("http://nonexistent-domain-12345.com/page#section");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with username and password")
    void testURLWithCredentials() {
        ContentValidatorResult result = rule.validate("http://user:pass@nonexistent-domain-12345.com");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle FTP URL")
    void testFTPURL() {
        ContentValidatorResult result = rule.validate("ftp://ftp.example.com/file.txt");

        assertFalse(result.isValid());
        // FTP is not HTTP/HTTPS, so will fail with MalformedURL or specific error
        assertNotNull(result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle very long URL")
    void testVeryLongURL() {
        String longPath = "a".repeat(1000);
        ContentValidatorResult result = rule.validate("http://nonexistent-domain-12345.com/" + longPath);

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with international domain name")
    void testInternationalDomainName() {
        ContentValidatorResult result = rule.validate("http://ñoño-nonexistent-12345.com");

        assertFalse(result.isValid());
        // Could be MalformedURL or UnknownHost
        assertNotNull(result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with unicode characters in path")
    void testUnicodeInPath() {
        ContentValidatorResult result = rule.validate("http://nonexistent-domain-12345.com/página");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle localhost URL")
    void testLocalhostURL() {
        // Localhost might or might not have a service running
        ContentValidatorResult result = rule.validate("http://localhost:99999");

        assertFalse(result.isValid());
        // Could be ConnectionError or ERROR since port likely not available
        assertTrue(result.getReceivedValue().equals("ConnectionError") || 
                   result.getReceivedValue().equals("ERROR") ||
                   result.getReceivedValue().equals("UnknownError"));
    }

    @Test
    @DisplayName("Should return not null received value")
    void testReceivedValueNotNull() {
        ContentValidatorResult result = rule.validate("invalid");

        assertNotNull(result);
        assertNotNull(result.getReceivedValue());
        assertFalse(result.isValid());
    }
}
