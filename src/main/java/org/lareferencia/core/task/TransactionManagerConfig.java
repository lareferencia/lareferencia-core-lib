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

import jakarta.persistence.EntityManagerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Configuration for primary transaction manager.
 * 
 * <p>
 * When Flowable is enabled, there are multiple transaction managers.
 * This configuration ensures the primary JPA datasource has a
 * transaction manager named 'transactionManager' that is marked as @Primary.
 * Uses JpaTransactionManager to properly support JPA/Hibernate transactions.
 * 
 * @author LA Referencia Team
 */
@Configuration
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class TransactionManagerConfig {

    /**
     * Creates the primary transaction manager for JPA operations.
     * Uses JpaTransactionManager (not DataSourceTransactionManager) to properly
     * support JPA transactions with Hibernate.
     * This is required when Flowable is enabled since it creates its own
     * flowableTransactionManager for the Flowable datasource.
     * 
     * @param entityManagerFactory the JPA entity manager factory
     * @return PlatformTransactionManager for JPA operations
     */
    @Bean(name = "transactionManager")
    @Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
