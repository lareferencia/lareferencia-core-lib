
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.validation.transformer.FieldContentTranslateRule;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.Profiler;
import org.lareferencia.core.validation.Translation;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;
import org.w3c.dom.Document;

import net.openhft.hashing.LongHashFunction;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MetadataTransformersUnitTests {
	
	
     
	@Test
    public void test_translation_rule_transformer() throws Exception {
    	   	  	
    	Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
    	OAIRecordMetadata metadata = new OAIRecordMetadata("dummy", doc);

    	//System.out.println(  metadata.toString() );
    	System.out.println(  metadata.getFieldOcurrences("dc.rights.*") );
    	assertFalse( metadata.getFieldOcurrences("dc.rights.*").contains("openAccess") );
    	
    	FieldContentTranslateRule trfTranslate = new FieldContentTranslateRule();
    	List<Translation> trfList = new ArrayList<Translation>();
    	
    	trfList.add( new Translation("http://purl.org/coar/access_right/c_abf2", "openAccess"));
    	trfList.add( new Translation("open access", "openAccess"));
    	trfTranslate.setTranslationArray(trfList);
    	trfTranslate.setTestFieldName("dc.rights.*");
    	trfTranslate.setWriteFieldName("dc.rights.none");
    	
    	trfTranslate.transform(null, null, metadata);
    	
    	System.out.println(  metadata.getFieldOcurrences("dc.rights.*") );
    	assertTrue( metadata.getFieldOcurrences("dc.rights.*").contains("openAccess") );
     		
    }
	
	@Test
    public void test_regex_rule_transformer() throws Exception {
    	   	  	
    	Document doc = getXmlDocumentFromResourcePath("original.xoai.record.xml");
    	OAIRecordMetadata metadata = new OAIRecordMetadata("dummy", doc);

    	//System.out.println(  metadata.toString() );
    	System.out.println(  metadata.getFieldOcurrences("dc.rights.*") );
    	assertFalse( metadata.getFieldOcurrences("dc.rights.*").contains("openAccess") );
    	
    	FieldContentTranslateRule trfTranslate = new FieldContentTranslateRule();
    	List<Translation> trfList = new ArrayList<Translation>();
    	
    	trfList.add( new Translation("http://purl.org/coar/access_right/c_abf2", "openAccess"));
    	trfList.add( new Translation("open access", "openAccess"));
    	trfTranslate.setTranslationArray(trfList);
    	trfTranslate.setTestFieldName("dc.rights.*");
    	trfTranslate.setWriteFieldName("dc.rights.none");
    	
    	trfTranslate.transform(null, null, metadata);
    	
    	System.out.println(  metadata.getFieldOcurrences("dc.rights.*") );
    	assertTrue( metadata.getFieldOcurrences("dc.rights.*").contains("openAccess") );
     		
    } 

    
    
    private Document getXmlDocumentFromResourcePath(String resourcePath) throws Exception {
    	
    	InputStream resource = new ClassPathResource(resourcePath).getInputStream();
    	DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = dBuilder.parse(resource);
		
    	return doc;
    }
    

}