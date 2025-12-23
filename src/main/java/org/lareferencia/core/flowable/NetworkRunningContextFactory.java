/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

package org.lareferencia.core.flowable;

import org.lareferencia.core.domain.Network;
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.lareferencia.core.worker.IRunningContext;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Factory implementation that creates NetworkRunningContext from process
 * variables.
 * <p>
 * Expects a 'networkId' variable in the process to look up the Network entity
 * and create the appropriate running context.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Component
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class NetworkRunningContextFactory implements IWorkerContextFactory {

    @Autowired
    private NetworkRepository networkRepository;

    /**
     * Creates a NetworkRunningContext from process variables.
     * 
     * @param variables must contain 'networkId' (Long) - the network ID to process
     * @return NetworkRunningContext for the specified network
     * @throws IllegalArgumentException if networkId is missing or network not found
     */
    @Override
    public IRunningContext createContext(Map<String, Object> variables) {
        Long networkId = (Long) variables.get("networkId");

        if (networkId == null) {
            throw new IllegalArgumentException("networkId is required in process variables");
        }

        Network network = networkRepository.findById(networkId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Network not found with id: " + networkId));

        return new NetworkRunningContext(network);
    }
}
