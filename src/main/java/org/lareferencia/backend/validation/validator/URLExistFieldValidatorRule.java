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

package org.lareferencia.backend.validation.validator;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.validation.AbstractValidatorFieldContentRule;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * Validation rule that checks if URLs in a metadata field exist and are accessible.
 * Performs HTTP HEAD requests to verify URL availability, following redirects automatically.
 */
@Getter
@Setter
public class URLExistFieldValidatorRule extends AbstractValidatorFieldContentRule {
	
	private static Logger logger = LogManager.getLogger(URLExistFieldValidatorRule.class);
	
	/**
	 * Constructs a new URLExistFieldValidatorRule instance.
	 */
	public URLExistFieldValidatorRule() {
	}
	
	/**
	 * Checks if the specified URL exists and returns HTTP 200 OK status.
	 * Automatically follows HTTP redirects (301, 302, 303) to verify final destination.
	 *
	 * @param url the URL to check for existence
	 * @return true if the URL exists and is accessible (HTTP 200), false otherwise
	 * @throws MalformedURLException if the URL format is invalid
	 * @throws UnknownHostException if the host cannot be resolved
	 * @throws IOException if an I/O error occurs during connection
	 */
	private boolean exists(String url) throws MalformedURLException, UnknownHostException, IOException  {
	
		HttpsURLConnection.setFollowRedirects(true);
		HttpURLConnection.setFollowRedirects(true);

		HttpURLConnection conn = getConnection(url);

		int responseCode = conn.getResponseCode();
		logger.debug("Verificando: " + url + " :: " + responseCode);

		// si es un redirect
		if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_MOVED_PERM
				|| responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
			String redirectUrl = conn.getHeaderField("Location");
			conn = getConnection(redirectUrl);
			responseCode = conn.getResponseCode();
			logger.debug("Verificando Redirect: " + redirectUrl + " :: " + responseCode);
		}

		return (responseCode == HttpURLConnection.HTTP_OK);	
	}

	private HttpURLConnection getConnection(String url) throws MalformedURLException, UnknownHostException, IOException   {
		
		
		HttpURLConnection con;

		 if ( url.startsWith("https") ) {
	    	  con = (HttpsURLConnection) new URL(url).openConnection();
	    	  con.setRequestMethod("HEAD");
	      }
	      else {
	    	  con  = (HttpURLConnection)  new URL(url).openConnection();
	    	  con.setRequestMethod("HEAD");
	      }
		 
		  return con;
	}

	@Override
	public ContentValidatorResult validate(String content) {
		
			ContentValidatorResult result = new ContentValidatorResult();
		
			if (content == null) {
				result.setReceivedValue("NULL");
				result.setValid(false);
			} else {
				
				boolean exists = false;
				
				try {
				
					exists = exists(content);
					result.setReceivedValue( exists ? "OK" : "ERROR" );
			
				} catch (MalformedURLException e) {
					result.setReceivedValue( "MalformedURL" );
				} catch (UnknownHostException e) {
					result.setReceivedValue( "UnknownHost" );
				} catch (IOException e) {
					result.setReceivedValue( "ConnectionError" );
				} catch (Exception e) {
					result.setReceivedValue( "UnknownError" );
				}
				
				result.setValid(exists);
			
			}

			return result;
		
	}

	@Override
	public String toString() {
		return "URLExistFieldValidatorRule [id=" + ruleId + ", field=" + getFieldname() + " , mandatory=" + mandatory + ", quantifier=" + quantifier + "]";
	}

}
