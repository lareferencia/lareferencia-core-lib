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

package org.lareferencia.core.repository.jpa;

import org.lareferencia.core.domain.ValidatorRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.data.repository.query.Param;

/**
 * JPA repository for managing {@link ValidatorRule} entities.
 * Provides standard CRUD operations for validation rules.
 */
@RepositoryRestResource(path = "validatorRule", collectionResourceRel = "validatorRule")
public interface ValidatorRuleRepository extends JpaRepository<ValidatorRule, Long> {

	long countByJsonserializationContaining(String packageName);

	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("update ValidatorRule r set r.jsonserialization = replace(r.jsonserialization, :oldPackage, :newPackage) where r.jsonserialization like concat('%', :oldPackage, '%')")
	int replaceJsonserializationPackage(
			@Param("oldPackage") String oldPackage,
			@Param("newPackage") String newPackage);

}
