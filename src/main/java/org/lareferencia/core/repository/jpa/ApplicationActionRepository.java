package org.lareferencia.core.repository.jpa;

import java.util.List;
import java.util.Optional;

import org.lareferencia.core.domain.ApplicationAction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

/** Internal repository. This policy is intentionally not a Data REST resource. */
@RepositoryRestResource(exported = false)
public interface ApplicationActionRepository extends JpaRepository<ApplicationAction, Long> {
    List<ApplicationAction> findAllByEngineTypeOrderByExecutionOrderAscActionKeyAsc(String engineType);

    Optional<ApplicationAction> findByEngineTypeAndActionKey(String engineType, String actionKey);

    long countByEngineType(String engineType);
}
