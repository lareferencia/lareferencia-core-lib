
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

package org.lareferencia.core.oabroker;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Configuration for the broker subsystem.
 * <p>
 * Enables JPA entity scanning and repository support for broker events
 * used to track and manage metadata changes and notifications.
 * </p>
 * 
 * @author LA Referencia Team
 */
@EntityScan( basePackages= { "org.lareferencia.core.oabroker" } )
@EnableJpaRepositories( basePackages={ "org.lareferencia.core.oabroker" } )
@Configuration(proxyBeanMethods = false)
public class BrokerConfiguration {

	// Constructor público necesario para Spring Boot 3.x CGLIB proxies
	// Se usa proxyBeanMethods=false para evitar la necesidad de proxies CGLIB
	
}
