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

package org.lareferencia.backend.taskmanager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.repositories.jpa.NetworkRepository;
import org.lareferencia.backend.repositories.jpa.OAIBitstreamRepository;
import org.lareferencia.backend.services.validation.ValidationStatisticsException;
import org.lareferencia.backend.services.validation.IValidationStatisticsService;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.Getter;
import lombok.Setter;

public class NetworkCleanWorker extends BaseWorker<NetworkRunningContext> {
	
	
	private static Logger logger = LogManager.getLogger(NetworkCleanWorker.class);


	@Override
	public String toString() {
		if ( !deleteEntireNetwork )
			return "Cleaner";
		else
			return "Delete";
	}

	@Autowired
	private IValidationStatisticsService validationStatisticsService;
		
	@Autowired
	private IMetadataRecordStoreService metadataStoreService;
	
	@Autowired
	private OAIBitstreamRepository bitstreamRepository;
	
	@Setter
	@Getter
	private boolean deleteEntireNetwork;
	
	@Autowired
	NetworkRepository networkRepository;

	public NetworkCleanWorker() {
		deleteEntireNetwork = false;
	};

	
	@Override
	public void run() {
		
		Network network = runningContext.getNetwork();
		
		// si no es una limpiza total debe identificar el ultimo snapshot cosechado y el ultimo válido para no limpiarlos
		if ( ! deleteEntireNetwork ) { 
			
			logger.info("Running Network/Repository cleanning process: " + network.getAcronym());
			
			Long lgkSnapshotID = metadataStoreService.findLastGoodKnownSnapshot(network);
			Long lhSnapshotID = metadataStoreService.findLastHarvestingSnapshot(network);
			
			// clean all snapshot data except last harvested and last good known snapshots
			for (Long snapshotId : metadataStoreService.listSnapshotsIds(network.getId(), false) ) {
				// si no es el lgk ni lh
				if ( !snapshotId.equals(lgkSnapshotID) && !snapshotId.equals(lhSnapshotID) ) {
					
					try {
						cleanSnapshotStatsData(snapshotId);
						metadataStoreService.cleanSnapshotData(snapshotId);

					} catch (ValidationStatisticsException e) {
						logger.error("Error cleanning snapshot" + e.getMessage());
					}
				
				}
			}
			
		}
		else { // caso de borrado completo de la red
			logger.info("Deleting the entire network/repository: " + network.getAcronym());
			
			// limpia todos los snapshots 
			for (Long snapshotId : metadataStoreService.listSnapshotsIds(network.getId(), true) ) {
				try {
					cleanSnapshotStatsData(snapshotId);
					metadataStoreService.cleanSnapshotData(snapshotId);
					metadataStoreService.deleteSnapshot(snapshotId);

				} catch (ValidationStatisticsException e) {
					logger.error("Error cleanning snapshot" + e.getMessage());
				}
			}
			
			// borra el bitstream de cosechas
			logger.debug("Deleting bitstreams");
			bitstreamRepository.deleteByNetworkID(network.getId());
			
			networkRepository.deleteByNetworkID(network.getId());
			logger.debug("Network/Repository deleted: " +network.getName()); 
		}

	}

	
	private void cleanSnapshotStatsData(Long snapshotId) throws ValidationStatisticsException {

		// borra los resultados de validación
		logger.debug("Deleting validation data for snapshot: " + snapshotId);
		validationStatisticsService.deleteValidationStatsObservationsBySnapshotID( snapshotId );

	}
	

	

}
