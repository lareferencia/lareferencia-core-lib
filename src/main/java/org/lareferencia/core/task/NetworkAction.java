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

package org.lareferencia.core.task;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents an action to be executed on a network.
 */
public class NetworkAction {

	/**
	 * Constructs a new network action with empty workers and properties lists.
	 */
	public NetworkAction() {
		workers = new ArrayList<>();
		properties = new ArrayList<>();
	}

	/**
	 * List of worker names to be executed as part of this action.
	 */
	@Getter
	@Setter
	List<String> workers;

	/**
	 * Flag indicating whether this action runs in incremental mode.
	 */
	private boolean incremental = false;

	/**
	 * List of properties associated with this network action.
	 */
	@Getter
	@Setter
	List<NetworkProperty> properties;

	/** Safe runtime configuration descriptors for the prototype workers in this action. */
	@Getter
	@Setter
	List<WorkerConfigurationDescriptor> workerConfigurations = new ArrayList<>();

	/**
	 * Flag indicating whether this action should run on schedule.
	 */
	@Getter
	@Setter
	Boolean runOnSchedule = false;

	/**
	 * Flag indicating whether this action should always run on schedule regardless
	 * of other conditions.
	 */
	@Getter
	@Setter
	Boolean allwaysRunOnSchedule = false;

	/**
	 * Installation-catalogue default used only the first time an engine is
	 * discovered. Existing actions keep the historical enabled-by-default
	 * behaviour; potentially destructive actions can explicitly opt out.
	 */
	@Getter
	@Setter
	boolean enabledByDefault = true;

	/**
	 * The name identifier of this action.
	 */
	@Getter
	@Setter
	String name = "DUMMY";

	/**
	 * A human-readable description of this action's purpose.
	 */
	@Getter
	@Setter
	String description = "DUMMY";

	/** Optional v5 catalog metadata; hidden from the legacy action DTO. */
	@JsonIgnore @Getter @Setter
	JsonNode configurationSchema;

	@JsonIgnore @Getter @Setter
	JsonNode uiSchema;

	@JsonIgnore @Getter @Setter
	JsonNode defaultConfiguration;

	@JsonIgnore @Getter @Setter
	String version;

	@JsonIgnore @Getter @Setter
	String processKey;

	/**
	 * Checks if this action operates in incremental mode.
	 *
	 * @return true if incremental mode is enabled, false otherwise
	 */
	public boolean isIncremental() {
		return incremental;
	}

	/**
	 * Sets whether this action should operate in incremental mode.
	 *
	 * @param incremental true to enable incremental mode, false otherwise
	 */
	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

	/**
	 * Returns the legacy properties that are execution modifiers rather than the
	 * switch that used to opt an action into the scheduled chain.
	 */
	@JsonIgnore
	public List<NetworkProperty> getConfigurationProperties() {
		if (properties == null || properties.isEmpty()) return List.of();
		if (Boolean.TRUE.equals(allwaysRunOnSchedule)) return List.copyOf(properties);
		return properties.size() > 1 ? List.copyOf(properties.subList(1, properties.size())) : List.of();
	}

}
