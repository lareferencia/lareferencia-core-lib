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

package org.lareferencia.core.worker.validation;

/**
 * Exception thrown when validation errors occur during record processing.
 */
public class ValidationException extends Exception {

	/**
	 * Constructs a new ValidationException with no detail message.
	 */
	public ValidationException() {
	}

	/**
	 * Constructs a new ValidationException with the specified detail message.
	 *
	 * @param message the detail message
	 */
	public ValidationException(String message) {
		super(message);
	}

	/**
	 * Constructs a new ValidationException with the specified cause.
	 *
	 * @param cause the cause of the exception
	 */
	public ValidationException(Throwable cause) {
		super(cause);
	}

	/**
	 * Constructs a new ValidationException with the specified detail message and cause.
	 *
	 * @param message the detail message
	 * @param cause the cause of the exception
	 */
	public ValidationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructs a new ValidationException with full control over exception handling.
	 *
	 * @param message the detail message
	 * @param cause the cause of the exception
	 * @param enableSuppression whether suppression is enabled
	 * @param writableStackTrace whether the stack trace should be writable
	 */
	public ValidationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
