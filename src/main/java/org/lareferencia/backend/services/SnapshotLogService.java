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

package org.lareferencia.backend.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.NetworkSnapshotLog;
import org.lareferencia.backend.repositories.jpa.NetworkSnapshotLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Scope("singleton")
public class SnapshotLogService {
	
	@Autowired
	private NetworkSnapshotLogRepository snapshotLogRepository;

	private static Logger logger = LogManager.getLogger(SnapshotLogService.class);

	public SnapshotLogService() {
	}
	
	@Transactional
	public void addEntry(Long snapshotId, String message) {
				
		if ( snapshotId != null && message != null) {
			logger.debug( "Loggin to SnapshotID: " + snapshotId + " message: " +  message );

			snapshotLogRepository.save(new NetworkSnapshotLog(message, snapshotId));
			snapshotLogRepository.flush();
		}
	}
	
	@Transactional
	public void deleteSnapshotLog(Long snapshotId) {
		logger.debug("Deleting log entries, snapshotId: " + snapshotId);
		if ( snapshotId != null )
			snapshotLogRepository.deleteBySnapshotID(snapshotId);
	}

}
