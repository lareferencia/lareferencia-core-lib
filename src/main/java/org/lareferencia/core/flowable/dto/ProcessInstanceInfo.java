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

package org.lareferencia.core.flowable.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * DTO containing information about a Flowable process instance.
 * 
 * @author LA Referencia Team
 */
@Data
@Builder
public class ProcessInstanceInfo {

    /** Unique identifier of the process instance */
    private String processInstanceId;

    /** Key of the process definition (e.g., "networkProcessing") */
    private String processDefinitionKey;

    /** Human-readable name of the process definition */
    private String processDefinitionName;

    /** Current process variables */
    private Map<String, Object> variables;

    /** ID of the current activity/task being executed */
    private String currentActivityId;

    /** Name of the current activity/task */
    private String currentActivityName;

    /** When the process instance was started */
    private LocalDateTime startTime;

    /** When the process instance completed (null if still running) */
    private LocalDateTime endTime;

    /** True if the process has completed */
    private boolean completed;

    /** True if the process is suspended */
    private boolean suspended;
}
