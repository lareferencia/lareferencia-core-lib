package org.lareferencia.core.task;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/** Discovers editable scalar JavaBean properties without exposing worker runtime state or dependencies. */
@Service
public class WorkerConfigurationIntrospector {
    private static final Set<String> RUNTIME_PROPERTIES = Set.of("class", "incremental", "runningContext", "scheduledFuture", "status", "id");
    private final ApplicationContext context;
    public WorkerConfigurationIntrospector(ApplicationContext context) { this.context = context; }

    public WorkerConfigurationDescriptor describe(String key, String beanName, WorkerConfigurationDescriptor declared) {
        WorkerConfigurationDescriptor result = new WorkerConfigurationDescriptor();
        result.setKey(key); result.setBeanName(beanName);
        Map<String, WorkerConfigurationProperty> properties = new LinkedHashMap<>();
        if (declared != null) declared.getProperties().forEach(property -> properties.put(property.getName(), property));
        Class<?> type = context.getType(beanName);
        Object instance = null;
        try { instance = context.getBean(beanName); } catch (RuntimeException ignored) { /* schema still uses its class */ }
        final Object workerInstance = instance;
        if (type != null) {
            try {
                for (PropertyDescriptor property : Introspector.getBeanInfo(type).getPropertyDescriptors()) {
                    if (RUNTIME_PROPERTIES.contains(property.getName()) || property.getWriteMethod() == null
                            || !supported(property.getPropertyType())) continue;
                    properties.computeIfAbsent(property.getName(), name -> property(name, property, workerInstance));
                }
            } catch (IntrospectionException exception) { throw new IllegalStateException("Cannot inspect worker " + beanName, exception); }
        }
        result.setProperties(properties.values().stream().toList());
        return result;
    }

    private WorkerConfigurationProperty property(String name, PropertyDescriptor descriptor, Object instance) {
        WorkerConfigurationProperty result = new WorkerConfigurationProperty(); result.setName(name);
        result.setDescription(Arrays.stream(name.split("(?=[A-Z])")).map(part -> part.substring(0, 1).toUpperCase() + part.substring(1)).reduce((a, b) -> a + " " + b).orElse(name));
        result.setType(schemaType(descriptor.getPropertyType()));
        Method read = descriptor.getReadMethod();
        if (instance != null && read != null) try { Object value = read.invoke(instance); if (value != null) result.setDefaultValue(String.valueOf(value)); } catch (ReflectiveOperationException ignored) { }
        return result;
    }
    private boolean supported(Class<?> type) { return type == String.class || type == boolean.class || type == Boolean.class || type.isEnum() || type == byte.class || type == Byte.class || type == short.class || type == Short.class || type == int.class || type == Integer.class || type == long.class || type == Long.class || type == float.class || type == Float.class || type == double.class || type == Double.class; }
    private String schemaType(Class<?> type) { if (type == boolean.class || type == Boolean.class) return "boolean"; if (type == float.class || type == Float.class || type == double.class || type == Double.class) return "number"; return type == String.class || type.isEnum() ? "string" : "integer"; }
}
