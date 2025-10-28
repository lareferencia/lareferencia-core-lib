
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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import javax.net.ssl.HttpsURLConnection;


import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Simple client for making HTTP/HTTPS GET requests and parsing JSON responses.
 * Provides basic functionality for retrieving JSON data from web services
 * without requiring complex HTTP client libraries.
 */
public class SimpleJWSClient {
	
	/**
	 * Constructs a new SimpleJWSClient instance.
	 */
    public SimpleJWSClient() {
		super();
	}

	/**
	 * Performs an HTTP GET request to the specified URL and parses the response as JSON.
	 * Automatically handles both HTTP and HTTPS connections.
	 *
	 * @param urlString the URL to send the GET request to
	 * @return the parsed JSON object from the response
	 * @throws IOException if an I/O error occurs during the request
	 * @throws ParseException if the response cannot be parsed as valid JSON
	 * @throws RuntimeException if the HTTP response code is not 200
	 */
	public Object get(String urlString) throws IOException, ParseException {
		
		URL url = new URL(urlString);	
			    
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			
		if ( urlString.startsWith("https") ) {
			conn = (HttpsURLConnection) new URL(urlString).openConnection();
	    }
	    else {
	    	conn  = (HttpURLConnection) new URL(urlString).openConnection();
	    }
	
		//conn.setRequestMethod("GET");
		//conn.connect();
		
		//Getting the response code
		int responsecode = conn.getResponseCode();
		
		if (responsecode != 200) {
		    throw new RuntimeException("HttpResponseCode: " + responsecode);
		} else {
		
		    String inline = "";
		    Scanner scanner = new Scanner(url.openStream());
		
		    //Write all the JSON data into a string using a scanner
		    while (scanner.hasNext()) {
		        inline += scanner.nextLine();
		    }
		
		    //Close the scanner
		    scanner.close();
		
		    //Using the JSON simple library parse the string into a json object
		    JSONParser parse = new JSONParser();
		    return  parse.parse(inline) ;
		
		}


	
	
	}
	
}
