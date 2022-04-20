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

package org.lareferencia.core.worker;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.OAIBitstream;
import org.lareferencia.backend.domain.OAIBitstreamStatus;
import org.lareferencia.backend.repositories.jpa.OAIBitstreamRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.List;

public class BitstreamPaginator implements IPaginator<OAIBitstream>  {

	// implements dummy starting page
	@Override
	public int getStartingPage() { return 1; }

	private static Logger logger = LogManager.getLogger(BitstreamPaginator.class);
	
	private static final int DEFAULT_PAGE_SIZE = 1000;
	
	private OAIBitstreamRepository repository;
	
	private int pageSize = DEFAULT_PAGE_SIZE;	
	private int totalPages = 0;
	
	private Network network;
	
	private OAIBitstreamStatus status;
	
	private boolean negateStatus = false;
	
	/**
	 * Este pedido paginado pide siempre la primera página
	 * restringida a que el id sea mayor al ultimo anterior
	 **/
	public BitstreamPaginator(OAIBitstreamRepository repository, Network network, OAIBitstreamStatus status, boolean negateStatus) {
		
		logger.debug("Creando paginador bistream - network: " + network.getAcronym() + " status: " + status + " negado?: " + negateStatus  );
		
		this.repository = repository;
		this.negateStatus = negateStatus;
		
		Page<OAIBitstream> page = repository.findByNetworkIdAndStatus(network.getId(), status, negateStatus, PageRequest.of(0, pageSize));
	
		this.totalPages = page.getTotalPages();
		this.status = status;
		this.network = network;

	}

	public BitstreamPaginator(OAIBitstreamRepository repository, Network network, OAIBitstreamStatus status) {
		this(repository, network, status, false);
	}
	
	 
	public BitstreamPaginator(OAIBitstreamRepository repository, Network network ) {
		
		logger.debug("Creando paginador bitstream - network: " + network.getAcronym() + " status: indistinto"  );
	
		this.repository = repository;
		Page<OAIBitstream> page = repository.findByNetworkId(network.getId(), PageRequest.of(0, pageSize));
		this.totalPages = page.getTotalPages();
	
		this.network = network;
		this.status = null;

	}
	
	public BitstreamPaginator() {
		
		this.totalPages = 0;
		this.status = null;

	}

	public int getTotalPages() {
		return totalPages;
	}

	public Page<OAIBitstream> nextPage() {
		Page<OAIBitstream> page; 
		
		if (status != null)
			page = repository.findByNetworkIdAndStatus(network.getId(), status, negateStatus, PageRequest.of(0, pageSize));
		else
			page = repository.findByNetworkId(network.getId(), PageRequest.of(0, pageSize));

		
		List<OAIBitstream> records = page.getContent();
		
		return page;
	}

	public void setPageSize(int size) {
		this.pageSize = size;
		
	}

}
