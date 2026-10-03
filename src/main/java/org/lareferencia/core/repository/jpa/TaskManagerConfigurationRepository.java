package org.lareferencia.core.repository.jpa;

import org.lareferencia.core.domain.TaskManagerConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(exported = false)
public interface TaskManagerConfigurationRepository extends JpaRepository<TaskManagerConfiguration, Long> { }
