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
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.OAIBitstream;
import org.lareferencia.backend.domain.OAIBitstreamStatus;
import org.lareferencia.backend.repositories.jpa.OAIBitstreamRepository;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.BitstreamPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.NumberFormat;


public class FulltextWorker extends BaseBatchWorker<OAIBitstream, NetworkRunningContext>  {
	
	
	private static Logger logger = LogManager.getLogger(FulltextWorker.class);
	
	@Value("${downloaded.files.path}")
	private String BITSTREAM_PATH;

	@Autowired
	OAIBitstreamRepository repository;
	
	
	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();
	
	@Autowired
	private OAIBitstreamRepository bitstreamRepository;


	public FulltextWorker() {
		super();
	
	}

	@Override
	public void preRun() {
		
		if ( runningContext.getNetwork() != null ) { // solo si existe el repositorio
			
			
			// si es incremental
			if ( this.isIncremental() ) {				
				logger.debug( "Fulltext de bitstreams (incremental)"  );
				// establece una paginator para recorrer NEW
				this.setPaginator( new BitstreamPaginator(repository, runningContext.getNetwork(), OAIBitstreamStatus.DOWNLOADED) );
			}			
			else {
				logger.debug( "Fulltext de bitstreams (full)"  );
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
		
		
			BodyContentHandler handler = new BodyContentHandler(-1);
			AutoDetectParser parser = new AutoDetectParser();
			Metadata metadata = new Metadata();
			File file = new File( BITSTREAM_PATH + "/" + bitstream.getId().getChecksum());
			try {
				FileInputStream fis = new FileInputStream(file);
				parser.parse(fis, handler, metadata);
				bitstream.setFulltext(handler.toString());
				bitstreamRepository.save(bitstream);
			} catch (SAXException e) {
				logger.error( "Problemas en extracción de texto del bitstream: " + bitstream.getId().getChecksum() + " : " + e.getMessage());
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				logger.error( "Problemas en extracción de texto del bitstream: " + bitstream.getId().getChecksum() + " : " + e.getMessage());
				e.printStackTrace();
			} catch (TikaException e) {
				logger.error( "Problemas en extracción de texto del bitstream: " + bitstream.getId().getChecksum() + " : " + e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				logger.error( "Problemas en extracción de texto del bitstream: " + bitstream.getId().getChecksum() + " : " + e.getMessage());
				e.printStackTrace();
			}
		
	}


	@Override
	public void postPage() {
		bitstreamRepository.flush();
		
	}

	@Override
	public void postRun() {
		bitstreamRepository.flush();
	}

	
	
	@Override
	public String toString() {
		return  "Fulltext extraction[" + percentajeFormat.format(this.getCompletionRate()) + "]";
	}
	
	
}
