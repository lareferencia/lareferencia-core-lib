package org.lareferencia.core.task;

import lombok.Getter;
import lombok.Setter;

/** An explicitly allow-listed, runtime-configurable property of a worker bean. */
@Getter
@Setter
public class WorkerConfigurationProperty {
    private String name;
    private String description;
    /** JSON Schema primitive type: boolean, integer, number or string. */
    private String type = "string";
    private String defaultValue;
}
