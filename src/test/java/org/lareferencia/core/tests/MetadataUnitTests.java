
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
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.Profiler;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;
import org.w3c.dom.Document;

import net.openhft.hashing.LongHashFunction;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MetadataUnitTests {
 
 
    
    
    @Test
    public void test_metadata_expresions() throws Exception {
    	
    	  	
    	Document doc = getXmlDocumentFromResourcePath("xoai_openaire.xml");
    	OAIRecordMetadata metadata = new OAIRecordMetadata("dummy", doc);
    	
    	System.out.println(  metadata.getFieldOcurrences("datacite.contributors.*:contributorType") );
    	
    	
    	
    } 
    
    
    private Document getXmlDocumentFromResourcePath(String resourcePath) throws Exception {
    	
    	InputStream resource = new ClassPathResource(resourcePath).getInputStream();
    	DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = dBuilder.parse(resource);
		
    	return doc;
    }
    
    
    @Test
    public void test_metadata_hash() throws Exception {
    	
    	  	
    	Profiler profiler = new Profiler(true, "");
    	
    	
    	Document doc = getXmlDocumentFromResourcePath("xoai_openaire.xml");
    	OAIRecordMetadata metadata = new OAIRecordMetadata("dummy", doc);
    	
//    	profiler.start();
//    	
//    	System.out.println( OAIRecord.calculateHash(metadata.toString()) );
//    	
//    	profiler.messure("md5");
//    	
    	long hash = LongHashFunction.xx().hashBytes(  metadata.toString().getBytes() );
    	 
    	
    	System.out.println( String.format("%016X", hash) );  
    	
    	profiler.messure("xxHash");
    	
    	hash = LongHashFunction.xx().hashBytes(  "test".getBytes() );
    	 
    	System.out.println( String.format("%016X", hash) );  

    	profiler.messure("xxHash2");

    	
    	System.out.println( profiler.report() );

    	
    	
    	
    } 

    

 
}