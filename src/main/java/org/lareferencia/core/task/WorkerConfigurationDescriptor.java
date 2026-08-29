package org.lareferencia.core.task;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Describes a prototype worker that may receive safe runtime overrides.
 * Bean names remain an internal Spring concern; only the stable key is public.
 */
@Getter
@Setter
public class WorkerConfigurationDescriptor {
    private String key;
    private String beanName;
    private List<WorkerConfigurationProperty> properties = new ArrayList<>();
}
