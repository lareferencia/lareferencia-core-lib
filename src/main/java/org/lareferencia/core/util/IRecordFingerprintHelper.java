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

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.validation.ValidationStatObservation;

/**
 * Interface for generating unique fingerprints and statistics IDs from records.
 * 
 * @author LA Referencia Team
 */
public interface IRecordFingerprintHelper {
	
	/**
	 * Gets the fingerprint for an OAI record.
	 * 
	 * @param record the OAI record
	 * @return the fingerprint string
	 */
	public String getFingerprint(OAIRecord record);

	/**
	 * Gets the fingerprint for a validation observation.
	 * 
	 * @param observation the validation observation
	 * @return the fingerprint string
	 */
	public String getFingerprint(ValidationStatObservation observation);

	/**
	 * Gets the statistics ID from an OAI record.
	 * 
	 * @param record the OAI record
	 * @return the stats ID string
	 */
	public String getStatsIDfromRecord(OAIRecord record);
	
	/**
	 * Gets the statistics ID from a validation observation.
	 * 
	 * @param observation the validation observation
	 * @return the stats ID string
	 */
	public String getStatsIDfromValidationStatObservation(ValidationStatObservation observation);


}
