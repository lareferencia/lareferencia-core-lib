
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

package org.lareferencia.core.tests;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.LocalDateTime;

import org.junit.Test;
import org.lareferencia.core.util.date.DateHelper;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DateParserUnitTests {
    
    
    @Autowired
    DateHelper dateHelper;
   
    @Test
    public void test_date_expresions() throws Exception {
    	
    	  
        LocalDateTime date = dateHelper.parseDate("2020");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );
        
        date = dateHelper.parseDate("2020-01");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020-01-01");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020-01-01T00:00");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020-01-01T00:00:00Z");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020-01-01T00:00:00+00:00");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020/01");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );

        date = dateHelper.parseDate("2020/01/01");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );
        
        date = dateHelper.parseDate("01/01/2020");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );
        
        date = dateHelper.parseDate("1/1/2020");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );
        
        date = dateHelper.parseDate("1/01/2020");
        System.out.println(date);
        assertEquals(date, LocalDateTime.parse("2020-01-01T00:00") );
        
    } 
   

 
}