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

package org.lareferencia.core.flowable.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for workflow management.
 * <p>
 * Loaded from application.yml under the "workflow" prefix.
 * </p>
 * 
 * <pre>
 * workflow:
 *   max-queued-processes: 32
 *   processes:
 *     networkProcessing:
 *       lane: null
 *     frontendIndexing:
 *       lane: 0
 *   lanes:
 *     0:
 *       name: "Global"
 *       max-queued: 10
 * </pre>
 * 
 * @author LA Referencia Team
 */
@Component
@ConfigurationProperties(prefix = "workflow")
@Data
public class WorkflowProperties {

    /** Maximum number of processes that can be queued globally */
    private int maxQueuedProcesses = 32;

    /** Configuration for each process type, keyed by process key */
    private Map<String, ProcessConfig> processes = new HashMap<>();

    /** Configuration for each lane, keyed by lane ID */
    private Map<Long, LaneConfig> lanes = new HashMap<>();

    /**
     * Gets the lane ID for a process type.
     * 
     * @param processKey the process key
     * @return the lane ID, or null for network queue
     */
    public Long getLaneForProcess(String processKey) {
        ProcessConfig config = processes.get(processKey);
        return config != null ? config.getLane() : null;
    }

    /**
     * Gets the max queued count for a lane.
     * 
     * @param laneId the lane ID
     * @return the max queued count, or 10 as default
     */
    public int getMaxQueuedForLane(Long laneId) {
        LaneConfig config = lanes.get(laneId);
        return config != null ? config.getMaxQueued() : 10;
    }
}
