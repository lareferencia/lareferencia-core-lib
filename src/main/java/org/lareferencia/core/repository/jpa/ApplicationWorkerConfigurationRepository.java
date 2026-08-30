package org.lareferencia.core.repository.jpa;

import java.util.List;
import java.util.Optional;

import org.lareferencia.core.domain.ApplicationWorkerConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(exported = false)
public interface ApplicationWorkerConfigurationRepository extends JpaRepository<ApplicationWorkerConfiguration, Long> {
    List<ApplicationWorkerConfiguration> findAllByEngineTypeOrderByWorkerKeyAsc(String engineType);
    Optional<ApplicationWorkerConfiguration> findByEngineTypeAndWorkerKey(String engineType, String workerKey);
}
