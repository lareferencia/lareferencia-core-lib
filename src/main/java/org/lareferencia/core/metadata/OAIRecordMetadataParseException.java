
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

package org.lareferencia.core.metadata;

/**
 * Exception thrown when parsing OAI record metadata fails.
 * Indicates errors in XML parsing or metadata structure validation.
 */
public class OAIRecordMetadataParseException extends Exception {


	private static final long serialVersionUID = 2279626648050333938L;

	/**
	 * Constructs a new OAI record metadata parse exception with no detail message.
	 */
	public OAIRecordMetadataParseException() {
		super();
	}

	/**
	 * Constructs a new OAI record metadata parse exception with the specified detail message and cause.
	 * 
	 * @param message the detail message describing the parsing error
	 * @param cause the cause of the exception
	 */
	public OAIRecordMetadataParseException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructs a new OAI record metadata parse exception with the specified detail message.
	 * 
	 * @param message the detail message describing the parsing error
	 */
	public OAIRecordMetadataParseException(String message) {
		super(message);
	}

	/**
	 * Constructs a new OAI record metadata parse exception with the specified cause.
	 * 
	 * @param cause the cause of the exception
	 */
	public OAIRecordMetadataParseException(Throwable cause) {
		super(cause);
	}

}