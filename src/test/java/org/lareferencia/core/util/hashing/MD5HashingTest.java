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

package org.lareferencia.core.util.hashing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MD5Hashing Unit Tests")
class MD5HashingTest {

    private MD5Hashing hashingHelper;

    @BeforeEach
    void setUp() {
        hashingHelper = new MD5Hashing();
    }

    @Test
    @DisplayName("Should calculate MD5 hash for non-empty string")
    void testCalculateHashNonEmpty() {
        String input = "test metadata";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
        assertEquals(32, hash.length()); // MD5 produces 32 character hex string
    }

    @Test
    @DisplayName("Should produce consistent hash for same input")
    void testCalculateHashConsistency() {
        String input = "consistent test data";
        
        String hash1 = hashingHelper.calculateHash(input);
        String hash2 = hashingHelper.calculateHash(input);
        
        assertEquals(hash1, hash2);
    }

    @Test
    @DisplayName("Should produce different hashes for different inputs")
    void testCalculateHashDifferentInputs() {
        String input1 = "test data 1";
        String input2 = "test data 2";
        
        String hash1 = hashingHelper.calculateHash(input1);
        String hash2 = hashingHelper.calculateHash(input2);
        
        assertNotEquals(hash1, hash2);
    }

    @Test
    @DisplayName("Should calculate hash for empty string")
    void testCalculateHashEmptyString() {
        String input = "";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
        assertEquals(32, hash.length());
        // MD5 of empty string is D41D8CD98F00B204E9800998ECF8427E
        assertEquals("D41D8CD98F00B204E9800998ECF8427E", hash);
    }

    @Test
    @DisplayName("Should calculate hash for long string")
    void testCalculateHashLongString() {
        String input = "a".repeat(10000);
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertEquals(32, hash.length());
    }

    @Test
    @DisplayName("Should calculate hash for string with special characters")
    void testCalculateHashSpecialCharacters() {
        String input = "Test with special chars: @#$%^&*()_+-=[]{}|;':\",./<>?";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertEquals(32, hash.length());
    }

    @Test
    @DisplayName("Should calculate hash for unicode string")
    void testCalculateHashUnicode() {
        String input = "Test unicode: ñáéíóú 中文 العربية 日本語";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertEquals(32, hash.length());
    }

    @Test
    @DisplayName("Should produce hash with only hex characters in uppercase")
    void testCalculateHashHexFormat() {
        String input = "test metadata";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertTrue(hash.matches("[0-9A-F]{32}"), "Hash should contain only uppercase hex characters");
    }

    @Test
    @DisplayName("Should implement IHashingHelper interface")
    void testImplementsInterface() {
        assertTrue(hashingHelper instanceof IHashingHelper);
    }

    @Test
    @DisplayName("Should handle whitespace correctly")
    void testCalculateHashWithWhitespace() {
        String input1 = "test";
        String input2 = "test ";
        
        String hash1 = hashingHelper.calculateHash(input1);
        String hash2 = hashingHelper.calculateHash(input2);
        
        assertNotEquals(hash1, hash2, "Hashes should differ due to trailing whitespace");
    }

    @Test
    @DisplayName("Should be case sensitive")
    void testCalculateHashCaseSensitive() {
        String input1 = "Test";
        String input2 = "test";
        
        String hash1 = hashingHelper.calculateHash(input1);
        String hash2 = hashingHelper.calculateHash(input2);
        
        assertNotEquals(hash1, hash2, "Hashes should differ due to case difference");
    }

    @Test
    @DisplayName("Should calculate known MD5 hash correctly")
    void testCalculateHashKnownValue() {
        String input = "hello";
        
        String hash = hashingHelper.calculateHash(input);
        
        // MD5 of "hello" is 5D41402ABC4B2A76B9719D911017C592
        assertEquals("5D41402ABC4B2A76B9719D911017C592", hash);
    }

    @Test
    @DisplayName("Should handle newline characters")
    void testCalculateHashWithNewlines() {
        String input1 = "line1\nline2";
        String input2 = "line1\r\nline2";
        
        String hash1 = hashingHelper.calculateHash(input1);
        String hash2 = hashingHelper.calculateHash(input2);
        
        assertNotEquals(hash1, hash2, "Different newline styles should produce different hashes");
    }

    @Test
    @DisplayName("Should handle tab characters")
    void testCalculateHashWithTabs() {
        String input = "test\tdata\twith\ttabs";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertEquals(32, hash.length());
    }

    @Test
    @DisplayName("Should handle numeric strings")
    void testCalculateHashNumericString() {
        String input = "1234567890";
        
        String hash = hashingHelper.calculateHash(input);
        
        assertNotNull(hash);
        assertEquals(32, hash.length());
    }

    @Test
    @DisplayName("Should produce consistent hashes for repeated calls")
    void testCalculateHashMultipleCalls() {
        String input = "test data";
        
        String hash1 = hashingHelper.calculateHash(input);
        String hash2 = hashingHelper.calculateHash(input);
        String hash3 = hashingHelper.calculateHash(input);
        
        assertEquals(hash1, hash2);
        assertEquals(hash2, hash3);
    }
}
