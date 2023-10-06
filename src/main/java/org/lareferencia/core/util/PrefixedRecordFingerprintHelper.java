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
import org.lareferencia.backend.domain.OAIRecord;

import lombok.Getter;
import lombok.Setter;
import org.lareferencia.backend.domain.ValidationStatObservation;

public class PrefixedRecordFingerprintHelper implements IRecordFingerprintHelper {

	@Getter
	@Setter
	private String prefix="";
	
	@Getter
	@Setter
	private Map<String,String> translateMap = new HashMap<String,String>();
	
	
	public PrefixedRecordFingerprintHelper() {
		this.prefix = "";
		translateMap = new HashMap<String,String>();
	}

	@Override
	public String getFingerprint(OAIRecord record) {
		

//oai:repositorio.bc.ufg.br:ri/7176 foi transformado em:
//oai:agregador.ibict.br.RI_UFG:oai:repositorio.bc.ufg.br:ri/7176
		
		
		if (record.getSnapshot() != null) {
			
			String networkAcronym = record.getSnapshot().getNetwork().getAcronym();
			
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
	public String getStatsIDfromRecord(OAIRecord record) {
		return record.getSnapshot().getId() + "-" + this.getFingerprint(record);

	}

	@Override
	public String getStatsIDfromValidationStatObservation(ValidationStatObservation observation) {
		return observation.getSnapshotID() + "-" + this.getFingerprint(observation);
	}


}
