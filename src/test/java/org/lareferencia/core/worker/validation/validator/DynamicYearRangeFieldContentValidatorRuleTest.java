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

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DynamicYearRangeFieldContentValidatorRule
 */
@DisplayName("DynamicYearRangeFieldContentValidatorRule Tests")
class DynamicYearRangeFieldContentValidatorRuleTest {

    private DynamicYearRangeFieldContentValidatorRule rule;
    private int currentYear;
    
    @BeforeEach
    void setUp() {
        rule = new DynamicYearRangeFieldContentValidatorRule();
        currentYear = LocalDate.now().getYear();
    }
    
    @Test
    @DisplayName("Should create rule with default regex")
    void testDefaultConstructor() {
        assertNotNull(rule);
        assertNotNull(rule.getRegexString());
    }
    
    @Test
    @DisplayName("Should validate current year")
    void testCurrentYear() {
        rule.setLowerLimit(0);
        rule.setUpperLimit(0);
        
        ContentValidatorResult result = rule.validate(String.valueOf(currentYear));
        
        assertTrue(result.isValid());
        assertEquals(String.valueOf(currentYear), result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should validate year within past range")
    void testYearInPastRange() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(0);
        
        int pastYear = currentYear - 5;
        ContentValidatorResult result = rule.validate(String.valueOf(pastYear));
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate year within future range")
    void testYearInFutureRange() {
        rule.setLowerLimit(0);
        rule.setUpperLimit(10);
        
        int futureYear = currentYear + 5;
        ContentValidatorResult result = rule.validate(String.valueOf(futureYear));
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate year too far in past")
    void testYearTooOld() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(0);
        
        int oldYear = currentYear - 15;
        ContentValidatorResult result = rule.validate(String.valueOf(oldYear));
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate year too far in future")
    void testYearTooFarFuture() {
        rule.setLowerLimit(0);
        rule.setUpperLimit(5);
        
        int futureYear = currentYear + 10;
        ContentValidatorResult result = rule.validate(String.valueOf(futureYear));
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should handle null content")
    void testNullContent() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(10);
        
        String nullString = null;
        ContentValidatorResult result = rule.validate(nullString);
        
        assertFalse(result.isValid());
        assertEquals("NULL or Empty", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle empty string")
    void testEmptyString() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(10);
        
        ContentValidatorResult result = rule.validate("");
        
        assertFalse(result.isValid());
        assertEquals("NULL or Empty", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should extract year from date string")
    void testExtractYearFromDate() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(10);
        
        String dateString = currentYear + "-10-27";
        ContentValidatorResult result = rule.validate(dateString);
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate 4-digit year")
    void testFourDigitYear() {
        rule.setLowerLimit(100);
        rule.setUpperLimit(10);
        
        ContentValidatorResult result = rule.validate("2023");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate 3-digit year")
    void testThreeDigitYear() {
        rule.setLowerLimit(2000);
        rule.setUpperLimit(10);
        
        ContentValidatorResult result = rule.validate("999");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should handle year in complex string")
    void testYearInComplexString() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(10);
        
        // The default regex extracts the first 3-4 digits, so we need the year at the beginning
        String complexString = currentYear + " University Publication";
        ContentValidatorResult result = rule.validate(complexString);
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate non-numeric year")
    void testNonNumericYear() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(10);
        
        ContentValidatorResult result = rule.validate("year2023");
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate exact lower boundary")
    void testExactLowerBoundary() {
        rule.setLowerLimit(10);
        rule.setUpperLimit(0);
        
        int boundaryYear = currentYear - 10;
        ContentValidatorResult result = rule.validate(String.valueOf(boundaryYear));
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate exact upper boundary")
    void testExactUpperBoundary() {
        rule.setLowerLimit(0);
        rule.setUpperLimit(10);
        
        int boundaryYear = currentYear + 10;
        ContentValidatorResult result = rule.validate(String.valueOf(boundaryYear));
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should set and get regex string")
    void testRegexStringProperty() {
        String customRegex = "^(\\d{4})";
        rule.setRegexString(customRegex);
        
        assertEquals(customRegex, rule.getRegexString());
    }
    
    @Test
    @DisplayName("Should set and get upper limit")
    void testUpperLimitProperty() {
        rule.setUpperLimit(20);
        assertEquals(20, rule.getUpperLimit());
    }
    
    @Test
    @DisplayName("Should set and get lower limit")
    void testLowerLimitProperty() {
        rule.setLowerLimit(50);
        assertEquals(50, rule.getLowerLimit());
    }
    
    @Test
    @DisplayName("Should validate publication year range")
    void testPublicationYearRange() {
        rule.setLowerLimit(100); // 100 years in the past
        rule.setUpperLimit(2);    // 2 years in the future
        
        // Should accept reasonable publication years
        assertTrue(rule.validate(String.valueOf(currentYear - 50)).isValid());
        assertTrue(rule.validate(String.valueOf(currentYear)).isValid());
        assertTrue(rule.validate(String.valueOf(currentYear + 1)).isValid());
        
        // Should reject very old or far future years
        assertFalse(rule.validate(String.valueOf(currentYear - 150)).isValid());
        assertFalse(rule.validate(String.valueOf(currentYear + 5)).isValid());
    }
    
    @Test
    @DisplayName("Should handle historical data range")
    void testHistoricalDataRange() {
        rule.setLowerLimit(500);
        rule.setUpperLimit(0);
        
        ContentValidatorResult medievalResult = rule.validate(String.valueOf(currentYear - 400));
        ContentValidatorResult ancientResult = rule.validate(String.valueOf(currentYear - 2000));
        
        assertTrue(medievalResult.isValid());
        assertFalse(ancientResult.isValid());
    }
}
