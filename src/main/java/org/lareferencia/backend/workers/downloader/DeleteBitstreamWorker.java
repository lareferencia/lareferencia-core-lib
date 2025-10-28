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

package org.lareferencia.backend.workers.downloader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.OAIBitstream;
import org.lareferencia.backend.domain.OAIBitstreamStatus;
import org.lareferencia.backend.repositories.jpa.OAIBitstreamRepository;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.BitstreamPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.text.NumberFormat;

/**
 * Worker that deletes bitstream files from the file system.
 * <p>
 * Processes bitstreams marked for deletion and removes their
 * corresponding files from storage.
 * </p>
 * 
 * @author LA Referencia Team
 * @see BaseBatchWorker
 * @see OAIBitstream
 */
public class DeleteBitstreamWorker extends BaseBatchWorker<OAIBitstream, NetworkRunningContext>  {
	
	
	private static Logger logger = LogManager.getLogger(DeleteBitstreamWorker.class);
	
	@Value("${downloaded.files.path}")
	private String BITSTREAM_PATH;

	@Autowired
	OAIBitstreamRepository repository;
	
	
	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();
	
	@Autowired
	private OAIBitstreamRepository bitstreamRepository;

	/**
	 * Creates a new delete bitstream worker.
	 */
	public DeleteBitstreamWorker() {
		super();

	
	}

	@Override
	public void preRun() {
		
		
		if ( runningContext.getNetwork() != null ) { // solo si existe el repositorio
			
			// si es incremental
			if ( this.isIncremental() ) {				
				logger.debug( "Delete Download de bitstreams (incremental)"  );
				// establece una paginator para recorrer NEW
				this.setPaginator( new BitstreamPaginator(repository, runningContext.getNetwork(), OAIBitstreamStatus.DOWNLOADED) );
			}			
			else {
				logger.debug( "Delete Download de bitstreams (full)"  );
				this.setPaginator( new BitstreamPaginator(repository, runningContext.getNetwork()) );
			}
				
		} else {
		
			logger.warn( "No red definida"  );
			this.setPaginator( new BitstreamPaginator() );
			this.stop();
		}
			
	}
	
	@Override
	public void prePage() {
	}
	
	@Override
	public void processItem(OAIBitstream bitstream) {
			
			File file = new File( BITSTREAM_PATH + "/" + bitstream.getId().getChecksum());
			file.delete();
			bitstream.setStatus(OAIBitstreamStatus.DELETED);
			bitstreamRepository.save(bitstream);
			
	}


	@Override
	public void postPage() {
		
	}

	@Override
	public void postRun() {
	
	}

	
	
	@Override
	public String toString() {
		return  "Delete File[" + percentajeFormat.format(this.getCompletionRate()) + "]";
	}
	
	
}
