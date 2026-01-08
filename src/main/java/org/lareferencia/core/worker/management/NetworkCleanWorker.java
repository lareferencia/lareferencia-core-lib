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

package org.lareferencia.core.worker.management;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.lareferencia.core.service.validation.IValidationStatisticsService;
import org.lareferencia.core.service.validation.ValidationStatisticsException;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.Getter;
import lombok.Setter;
import org.lareferencia.core.repository.catalog.OAIRecordCatalogRepository;

/**
 * Worker that cleans network snapshot data or deletes an entire network.
 * Removes records, metadata, and associated resources based on configuration.
 */
public class NetworkCleanWorker extends BaseWorker<NetworkRunningContext> {

	private static Logger logger = LogManager.getLogger(NetworkCleanWorker.class);

	@Override
	public String toString() {
		if (!deleteEntireNetwork)
			return "Cleaner";
		else
			return "Delete";
	}

	@Autowired
	private IValidationStatisticsService validationStatisticsService;

	@Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private OAIRecordCatalogRepository catalogRepo;

	/**
	 * Flag indicating whether to delete the entire network or just clean snapshot
	 * data.
	 */
	@Setter
	@Getter
	private boolean deleteEntireNetwork;

	/**
	 * Repository for accessing network entities.
	 */
	@Autowired
	NetworkRepository networkRepository;

	/**
	 * Constructs a new network clean worker with default settings.
	 * By default, only cleans snapshot data without deleting the entire network.
	 */
	public NetworkCleanWorker() {
		deleteEntireNetwork = false;
	};

	@Override
	public void run() {

		Network network = runningContext.getNetwork();

		// si no es una limpiza total debe identificar el ultimo snapshot cosechado y el
		// ultimo válido para no limpiarlos
		if (!deleteEntireNetwork) {

			logger.info("Running Network/Repository cleanning process: " + network.getAcronym());

			Long lgkSnapshotID = snapshotStore.findLastGoodKnownSnapshot(network);
			Long lhSnapshotID = snapshotStore.findLastHarvestingSnapshot(network);

			// clean all snapshot data except last harvested and last good known snapshots
			for (Long snapshotId : snapshotStore.listSnapshotsIds(network.getId(), false)) {
				// si no es el lgk ni lh
				if (!snapshotId.equals(lgkSnapshotID) && !snapshotId.equals(lhSnapshotID)) {

					try {
						cleanSnapshotStatsData(snapshotId);
						catalogRepo.deleteSnapshot(snapshotStore.getSnapshotMetadata(snapshotId));
						snapshotStore.cleanSnapshotData(snapshotId);

					} catch (Exception e) { // Broadened to catch IOException too
						logger.error("Error cleaning snapshot " + snapshotId + ": " + e.getMessage(), e);
					}

				}
			}

		} else { // caso de borrado completo de la red
			logger.info("Deleting the entire network/repository: " + network.getAcronym());

			// limpia todos los snapshots
			for (Long snapshotId : snapshotStore.listSnapshotsIds(network.getId(), true)) {
				try {
					cleanSnapshotStatsData(snapshotId);
					catalogRepo.deleteSnapshot(snapshotStore.getSnapshotMetadata(snapshotId));
					snapshotStore.cleanSnapshotData(snapshotId);
					snapshotStore.deleteSnapshot(snapshotId);

				} catch (Exception e) { // Broadened to catch IOException too
					logger.error("Error deleting snapshot " + snapshotId + ": " + e.getMessage(), e);
				}
			}

			networkRepository.deleteByNetworkID(network.getId());
			logger.debug("Network/Repository deleted: " + network.getName());
		}

	}

	private void cleanSnapshotStatsData(Long snapshotId) throws ValidationStatisticsException {

		// Delete validation results using new multi-file architecture
		logger.info("CLEAN WORKER: Deleting validation data for snapshot: " + snapshotId);
		validationStatisticsService.deleteValidationStatsObservationsBySnapshotID(snapshotId);
		logger.info("CLEAN WORKER: Successfully deleted validation data for snapshot: " + snapshotId);

	}

}
