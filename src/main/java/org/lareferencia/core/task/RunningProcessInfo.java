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

package org.lareferencia.core.task;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * DTO containing information about a running process/worker.
 * <p>
 * This class provides a unified view of running processes across
 * both legacy (TaskManager) and Flowable (WorkflowService) implementations.
 * </p>
 *
 * @author LA Referencia Team
 */
@Data
@Builder
public class RunningProcessInfo {

    /**
     * Unique identifier for the process.
     * For legacy: runningContextID
     * For Flowable: processInstanceId
     */
    private String processId;

    /**
     * The network acronym this process is running for.
     */
    private String networkAcronym;

    /**
     * The type of action/workflow being executed.
     */
    private String actionType;

    /**
     * Human-readable description of current status.
     */
    private String status;

    /**
     * When the process started.
     */
    private LocalDateTime startTime;

    /**
     * Whether the process is running incrementally.
     */
    private Boolean incremental;

    /**
     * Additional variables/context for the process.
     */
    private Map<String, Object> variables;

    /**
     * The execution engine type: "legacy" or "flowable"
     */
    private String engineType;
}
