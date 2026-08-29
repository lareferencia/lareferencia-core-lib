package org.lareferencia.core.repository.jpa;

import java.util.List;
import java.util.Optional;

import org.lareferencia.core.domain.NetworkActionConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(exported = false)
public interface NetworkActionConfigurationRepository extends JpaRepository<NetworkActionConfiguration, Long> {
    List<NetworkActionConfiguration> findAllByNetworkId(Long networkId);
    Optional<NetworkActionConfiguration> findByNetworkIdAndApplicationActionId(Long networkId, Long applicationActionId);
}
