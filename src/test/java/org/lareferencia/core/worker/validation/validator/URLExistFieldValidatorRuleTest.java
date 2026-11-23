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
    @DisplayName("Should handle HTTPS URL")
    void testHTTPSURL() {
        ContentValidatorResult result = rule.validate("https://unreachable.invalid");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with port")
    void testURLWithPort() {
        ContentValidatorResult result = rule.validate("http://unreachable.invalid:8080");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with fragment")
    void testURLWithFragment() {
        ContentValidatorResult result = rule.validate("http://unreachable.invalid/page#section");

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with username and password")
    void testURLWithCredentials() {
        ContentValidatorResult result = rule.validate("http://user:pass@unreachable.invalid");

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
        ContentValidatorResult result = rule.validate("http://unreachable.invalid/" + longPath);

        assertFalse(result.isValid());
        assertEquals("UnknownHost", result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with international domain name")
    void testInternationalDomainName() {
        ContentValidatorResult result = rule.validate("http://ñoño.invalid");

        assertFalse(result.isValid());
        // Could be MalformedURL or UnknownHost
        assertNotNull(result.getReceivedValue());
    }

    @Test
    @DisplayName("Should handle URL with unicode characters in path")
    void testUnicodeInPath() {
        ContentValidatorResult result = rule.validate("http://unreachable.invalid/página");

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
    @Test
    @org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.SYSTEM_PROPERTIES)
    void followsRedirectsUsesHeadAndBoundsTheRead() throws Exception {
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        var method = new java.util.concurrent.atomic.AtomicReference<String>();
        server.createContext("/ok", exchange -> {
            method.set(exchange.getRequestMethod());
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().set("Location", "/ok");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/missing", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        var release = new java.util.concurrent.CountDownLatch(1);
        server.createContext("/waiting", exchange -> {
            try { release.await(5, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        String key = "lareferencia.validation.url.readTimeoutMillis";
        String previous = System.getProperty(key);
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            assertTrue(rule.validate(base + "/ok?param=value&other=123").isValid());
            assertTrue(rule.validate(base + "/redirect").isValid());
            assertEquals("HEAD", method.get());
            assertFalse(rule.validate(base + "/missing").isValid());
            System.setProperty(key, "150");
            var timed = new URLExistFieldValidatorRule();
            assertTimeout(java.time.Duration.ofSeconds(2), () ->
                    assertEquals("ConnectionError", timed.validate(base + "/waiting").getReceivedValue()));
        } finally {
            release.countDown();
            server.stop(0);
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        }
    }
}
