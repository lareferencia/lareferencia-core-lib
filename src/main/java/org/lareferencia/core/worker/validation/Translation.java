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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a translation rule for string transformation.
 * Maps a search pattern to its replacement value.
 */
@Setter
@Getter
@ToString
public class Translation {

	@JsonProperty("search")
	String search;

	@JsonProperty("replace")
	String replace;

	/**
	 * Constructs a new Translation with search and replace patterns.
	 *
	 * @param search the search pattern
	 * @param replace the replacement value
	 */
	public Translation(@JsonProperty("search") String search, @JsonProperty("replace") String replace) {
		super();
		this.search = search;
		this.replace = replace;
	}

	/**
	 * Constructs a new Translation with default values.
	 */
	public Translation() {
		super();
	}

}