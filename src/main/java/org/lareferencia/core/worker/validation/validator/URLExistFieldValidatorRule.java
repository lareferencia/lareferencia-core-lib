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

import org.lareferencia.core.worker.validation.AbstractValidatorFieldContentRule;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * Validation rule that checks if URLs in a metadata field exist and are
 * accessible.
 * Performs HTTP HEAD requests to verify URL availability, following redirects
 * automatically.
 */
@ValidatorRuleMeta
public class URLExistFieldValidatorRule extends AbstractValidatorFieldContentRule {

	/**
	 * Constructs a new URLExistFieldValidatorRule instance.
	 */
	public URLExistFieldValidatorRule() {
	}

	/**
	 * Checks if the specified URL exists and returns HTTP 200 OK status.
	 * Automatically follows HTTP redirects (301, 302, 303) to verify final
	 * destination.
	 *
	 * @param url the URL to check for existence
	 * @return true if the URL exists and is accessible (HTTP 200), false otherwise
	 * @throws MalformedURLException if the URL format is invalid
	 * @throws UnknownHostException  if the host cannot be resolved
	 * @throws IOException           if an I/O error occurs during connection
	 */
	private boolean exists(String url) throws MalformedURLException, UnknownHostException, IOException {

        HttpURLConnection connection = getConnection(new URL(url));
        try {
            int status = connection.getResponseCode();
            if (status == HttpURLConnection.HTTP_MOVED_TEMP || status == HttpURLConnection.HTTP_MOVED_PERM
                    || status == HttpURLConnection.HTTP_SEE_OTHER) {
                URL target = new URL(connection.getURL(), connection.getHeaderField("Location"));
                connection.disconnect();
                connection = getConnection(target);
                status = connection.getResponseCode();
            }
            return status == HttpURLConnection.HTTP_OK;
        } finally {
            connection.disconnect();
        }
    }

    private HttpURLConnection getConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.setInstanceFollowRedirects(true);
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        return connection;
    }

    private final int connectTimeout = timeout("lareferencia.validation.url.connectTimeoutMillis", 5000);
    private final int readTimeout = timeout("lareferencia.validation.url.readTimeoutMillis", 10000);

    private static int timeout(String property, int fallback) {
        int millis = Integer.parseInt(System.getProperty(property, Integer.toString(fallback)));
        if (millis < 1) throw new IllegalArgumentException(property + " must be positive");
        return millis;
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
				result.setReceivedValue(exists ? "OK" : "ERROR");

			} catch (MalformedURLException e) {
				result.setReceivedValue("MalformedURL");
			} catch (UnknownHostException e) {
				result.setReceivedValue("UnknownHost");
			} catch (IOException e) {
				result.setReceivedValue("ConnectionError");
			} catch (Exception e) {
				result.setReceivedValue("UnknownError");
			}

			result.setValid(exists);

		}

		return result;

	}

	@Override
	public String toString() {
		return "URLExistFieldValidatorRule [id=" + ruleId + ", field=" + getFieldname() + " , mandatory=" + mandatory
				+ ", quantifier=" + quantifier + "]";
	}

}
