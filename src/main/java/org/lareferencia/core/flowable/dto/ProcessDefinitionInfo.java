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

/**
 * DTO containing information about a Flowable process definition.
 * 
 * @author LA Referencia Team
 */
@Data
@Builder
public class ProcessDefinitionInfo {

    /** Unique identifier of the process definition */
    private String processDefinitionId;

    /** Key of the process (used to start instances) */
    private String processKey;

    /** Human-readable name of the process */
    private String name;

    /** Description of what this process does */
    private String description;

    /** Version number (latest by default) */
    private int version;
}
