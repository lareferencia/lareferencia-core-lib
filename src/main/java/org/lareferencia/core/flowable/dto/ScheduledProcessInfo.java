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
 * Information about a scheduled process.
 * 
 * @author LA Referencia Team
 */
@Data
@Builder
public class ScheduledProcessInfo {

    /** Unique identifier for this schedule */
    private String scheduleId;

    /** Process definition key to execute */
    private String processKey;

    /** Cron expression for scheduling */
    private String cronExpression;

    /** Lane ID for queuing */
    private String laneId;

    /** Variables to pass to the process */
    private Map<String, Object> variables;

    /** Whether this schedule is currently enabled */
    private boolean enabled;

    /** When this schedule was created */
    private LocalDateTime createdAt;

    /** Next scheduled execution time */
    private LocalDateTime nextExecutionTime;
}
