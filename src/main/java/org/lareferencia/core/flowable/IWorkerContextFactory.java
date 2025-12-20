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

import org.lareferencia.core.worker.IRunningContext;

import java.util.Map;

/**
 * Factory interface for creating worker running contexts from process
 * variables.
 * <p>
 * This interface abstracts the creation of {@link IRunningContext} instances
 * from Flowable process variables, allowing different context types to be
 * created based on the specific workflow requirements.
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface IWorkerContextFactory {

    /**
     * Creates a running context from the given process variables.
     * 
     * @param variables the process variables containing context information
     * @return the created running context
     * @throws IllegalArgumentException if required variables are missing
     */
    IRunningContext createContext(Map<String, Object> variables);
}
