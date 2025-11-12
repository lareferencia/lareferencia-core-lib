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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.service.validation.ValidationStatObservation;
import org.lareferencia.core.metadata.SnapshotMetadata;

import lombok.Getter;
import lombok.Setter;

/**
 * Helper for generating record fingerprints with configurable prefix and network acronym translation.
 * Transforms record identifiers by adding prefixes and optionally translating network acronyms
 * for fingerprint uniqueness and traceability.
 */
public class PrefixedRecordFingerprintHelper implements IRecordFingerprintHelper {

	/**
	 * Prefix to prepend to record identifiers when generating fingerprints.
	 */
	@Getter
	@Setter
	private String prefix="";
	
	/**
	 * Map for translating network acronyms in fingerprint generation.
	 * Keys are original network acronyms, values are translated forms.
	 */
	@Getter
	@Setter
	private Map<String,String> translateMap = new HashMap<String,String>();
	
	/**
	 * Constructs a new PrefixedRecordFingerprintHelper with empty prefix and translation map.
	 */
	public PrefixedRecordFingerprintHelper() {
		this.prefix = "";
		translateMap = new HashMap<String,String>();
	}

	/**
	 * Generates a unique fingerprint for the given record by applying prefix and optional
	 * network acronym translation to the record identifier.
	 *
	 * @param record the OAI record to generate a fingerprint for
	 * @return the generated fingerprint string
	 */
	@Override
	public String getFingerprint(IOAIRecord record, SnapshotMetadata snapshotMetadata) {
		
		
		if (snapshotMetadata.getNetworkAcronym() != null) {
			
			String networkAcronym = snapshotMetadata.getNetworkAcronym();
			
			String new_identifier = prefix + record.getIdentifier();
			
			if ( translateMap != null && translateMap.containsKey(networkAcronym) ) 	
				new_identifier = prefix + "." + translateMap.get(networkAcronym) + ":" + record.getIdentifier() ;
				
				
			return  networkAcronym + "_" + DigestUtils.md5Hex(new_identifier);
			
		}
		
		else
			return "00" + "_" + DigestUtils.md5Hex(prefix + record.getIdentifier());

	
	}

	public String getFingerprint(ValidationStatObservation observation) {

		if (observation.getNetworkAcronym() != null) {

			String networkAcronym = observation.getNetworkAcronym();
			String new_identifier = prefix + observation.getIdentifier();

			if ( translateMap != null && translateMap.containsKey(networkAcronym) )
				new_identifier = prefix + "." + translateMap.get(networkAcronym) + ":" + observation.getIdentifier() ;


			return  networkAcronym + "_" + DigestUtils.md5Hex(new_identifier);

		}

		else
			return "00" + "_" + DigestUtils.md5Hex(prefix + observation.getIdentifier());


	}

	@Override
	public String getStatsIDfromRecord(IOAIRecord record, SnapshotMetadata snapshotMetadata) {
		return snapshotMetadata.getSnapshotId() + "-" + this.getFingerprint(record, snapshotMetadata);

	}

	@Override
	public String getStatsIDfromValidationStatObservation(ValidationStatObservation observation) {
		return observation.getSnapshotID() + "-" + this.getFingerprint(observation);
	}


}
