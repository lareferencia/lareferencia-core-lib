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

package org.lareferencia.core.worker.harvesting;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.OAIBitstream;
import org.lareferencia.core.domain.OAIBitstreamStatus;
import org.lareferencia.core.repository.jpa.OAIBitstreamRepository;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.BitstreamPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.NumberFormat;

/**
 * Worker that downloads bitstreams from remote URLs.
 * <p>
 * Processes bitstream URLs and downloads their content to local storage.
 * Supports incremental downloads and configurable timeouts.
 * </p>
 * 
 * @author LA Referencia Team
 * @see BaseBatchWorker
 * @see OAIBitstream
 */
public class DownloaderWorker extends BaseBatchWorker<OAIBitstream, NetworkRunningContext>  {
	
	
	private static Logger logger = LogManager.getLogger(DownloaderWorker.class);


	@Autowired
	OAIBitstreamRepository repository;
	
	@Getter @Setter
	private int timeOut;
	
	@Getter @Setter
	private String targetDirectory;
	
	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();
	
	@Autowired
	private OAIBitstreamRepository bitstreamRepository;

	/**
	 * Creates a new downloader worker with default timeout.
	 */
	public DownloaderWorker() {
		super();
		this.timeOut = 10000;
	
	}

	@Override
	public void preRun() {
		
		HttpURLConnection.setFollowRedirects(true);
		
		if ( runningContext.getNetwork() != null ) { // solo si existe el repositorio
			
			
			// si es incremental
			if ( this.isIncremental() ) {				
				logger.debug( "Download de bitstreams (incremental)"  );
				// establece una paginator para recorrer NEW
				this.setPaginator( new BitstreamPaginator(repository, runningContext.getNetwork(), OAIBitstreamStatus.NEW) );
			}			
			else {
				logger.debug( "Download de bitstreams (full)"  );
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
		
		
		try {
			
			logger.debug("Descargando Bitstream: " + bitstream.getId().getIdentifier() + " -- " + bitstream.getUrl() );
			
			
			String url = bitstream.getUrl();
			
			URL urlObj = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
			conn.setReadTimeout(5000);
			conn.setInstanceFollowRedirects(true);
			
			logger.debug("Request URL ... " + url);

			boolean redirect = false;

			// normally, 3xx is redirect
			int status = conn.getResponseCode();
			
			if (status != HttpURLConnection.HTTP_OK) {
				if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM
						|| status == HttpURLConnection.HTTP_SEE_OTHER)
				redirect = true;
			}

			logger.debug("Response Code ... " + status + "will redirect? "+redirect);

			if (redirect) {
				// get redirect url from "location" header field
				url = conn.getHeaderField("Location");
				logger.debug("Redirect to URL : " + url);

			}
			
			FileUtils.copyURLToFile(new URL(url), 
									new File(getTargetDirectory(), bitstream.getId().getChecksum() ), timeOut, timeOut
									);
			
			bitstream.setStatus( OAIBitstreamStatus.DOWNLOADED );
			bitstreamRepository.save(bitstream);
		
		} catch (Exception e) {
			logger.error( "Se detectaron problemas en el proceso de descarga de ODs " + runningContext.getNetwork().getAcronym() );
			logger.error( e.getMessage() );
			this.stop();
			e.printStackTrace();
		} 
		
		
	}


	@Override
	public void postPage() {
	
	}

	@Override
	public void postRun() {
		bitstreamRepository.flush();
	}

	
	
	@Override
	public String toString() {
		return  "Downloader[" + percentajeFormat.format(this.getCompletionRate()) + ""; 
	}
	
	
}
