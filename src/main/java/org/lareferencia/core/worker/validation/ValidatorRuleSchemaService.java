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

package org.lareferencia.core.worker.validation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.annotation.PostConstruct;

/**
 * Service for generating JSON schemas and forms from validator rule classes.
 * Uses Spring's classpath scanning to find annotated classes and builds schema
 * definitions at runtime.
 */
@Service
public class ValidatorRuleSchemaService {

    private static final Logger logger = LogManager.getLogger(ValidatorRuleSchemaService.class);

    private static final String BASE_PACKAGE = "org.lareferencia";

    private List<RuleSchemaDefinition> cachedValidatorSchemas;
    private List<RuleSchemaDefinition> cachedTransformerSchemas;

    @Autowired
    private MessageSource messageSource;

    @PostConstruct
    public void init() {
        logger.info("VALIDATOR RULE SERVICE: Initializing | Scanning validator and transformer classes...");
        this.cachedValidatorSchemas = scanAndBuildSchemas(IValidatorRule.class);
        this.cachedTransformerSchemas = scanAndBuildSchemas(ITransformerRule.class);
        logger.info("VALIDATOR RULE SERVICE: Scan Complete | Validator Rules: {} | Transformer Rules: {}",
                cachedValidatorSchemas.size(),
                cachedTransformerSchemas.size());
    }

    /**
     * Returns all validator rule schema definitions.
     */
    /**
     * Returns all validator rule schema definitions.
     */
    public List<RuleSchemaDefinition> getAllValidatorSchemas(Locale locale) {
        return buildLocalizedSchemas(cachedValidatorSchemas, locale);
    }

    /**
     * Returns all transformer rule schema definitions.
     */
    public List<RuleSchemaDefinition> getAllTransformerSchemas(Locale locale) {
        return buildLocalizedSchemas(cachedTransformerSchemas, locale);
    }

    /**
     * Returns schema definition for a specific class.
     */
    public RuleSchemaDefinition getSchemaForClass(String className, Locale locale) {
        RuleSchemaDefinition definition = cachedValidatorSchemas.stream()
                .filter(s -> s.getClassName().equals(className))
                .findFirst()
                .orElse(null);

        if (definition == null) {
            definition = cachedTransformerSchemas.stream()
                    .filter(s -> s.getClassName().equals(className))
                    .findFirst()
                    .orElse(null);
        }

        if (definition != null) {
            return buildLocalizedSchema(definition, locale);
        }

        return null;
    }

    private List<RuleSchemaDefinition> scanAndBuildSchemas(Class<?> interfaceType) {
        List<RuleSchemaDefinition> schemas = new ArrayList<>();

        try {
            ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(
                    false);
            scanner.addIncludeFilter(new AssignableTypeFilter(interfaceType));
            scanner.addIncludeFilter(new AnnotationTypeFilter(ValidatorRuleMeta.class));

            Set<BeanDefinition> candidates = scanner.findCandidateComponents(BASE_PACKAGE);

            for (BeanDefinition bd : candidates) {
                try {
                    Class<?> clazz = Class.forName(bd.getBeanClassName());

                    // Filter out class if it does not implement the interface
                    if (!interfaceType.isAssignableFrom(clazz)) {
                        continue;
                    }

                    RuleSchemaDefinition schema = buildSchemaForClass(clazz);
                    if (schema != null) {
                        schemas.add(schema);
                        logger.debug("Built schema for: {}", clazz.getName());
                    }
                } catch (ClassNotFoundException e) {
                    logger.error("Class not found: {}", bd.getBeanClassName());
                } catch (Exception e) {
                    logger.error("Error building schema for class {}: {}", bd.getBeanClassName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning classes for interface {}: {}", interfaceType.getName(), e.getMessage());
        }

        return schemas;
    }

    private RuleSchemaDefinition buildSchemaForClass(Class<?> clazz) {
        ValidatorRuleMeta meta = clazz.getAnnotation(ValidatorRuleMeta.class);
        if (meta == null) {
            return null;
        }

        RuleSchemaDefinition definition = new RuleSchemaDefinition();
        definition.setName(meta.name()); // Default name from annotation
        definition.setClassName(clazz.getName());

        // Collect fields from class hierarchy
        List<FieldInfo> fields = collectAnnotatedFields(clazz);

        // Sort by order
        fields.sort(Comparator.comparingInt(f -> f.order));

        // Build schema (structure only, localization happens later)
        definition.setSchema(buildJsonSchema(fields, null));

        // Build form (structure only)
        definition.setForm(buildForm(fields, meta.help(), null));

        return definition;
    }

    private List<RuleSchemaDefinition> buildLocalizedSchemas(List<RuleSchemaDefinition> definitions, Locale locale) {
        List<RuleSchemaDefinition> localized = new ArrayList<>();
        for (RuleSchemaDefinition def : definitions) {
            localized.add(buildLocalizedSchema(def, locale));
        }
        return localized;
    }

    private RuleSchemaDefinition buildLocalizedSchema(RuleSchemaDefinition original, Locale locale) {
        try {
            Class<?> clazz = Class.forName(original.getClassName());
            ValidatorRuleMeta meta = clazz.getAnnotation(ValidatorRuleMeta.class);

            RuleSchemaDefinition localized = new RuleSchemaDefinition();
            localized.setClassName(original.getClassName());

            // Localize Name
            String nameKey = "rule." + clazz.getSimpleName() + ".name";
            localized.setName(messageSource.getMessage(nameKey, null, meta.name(), locale));

            // Re-collect fields to rebuild schema/form with localization
            List<FieldInfo> fields = collectAnnotatedFields(clazz);
            fields.sort(Comparator.comparingInt(f -> f.order));

            localized.setSchema(buildJsonSchema(fields, locale));

            String helpKey = "rule." + clazz.getSimpleName() + ".help";
            String helpText = messageSource.getMessage(helpKey, null, meta.help(), locale);

            localized.setForm(buildForm(fields, helpText, locale));

            return localized;

        } catch (ClassNotFoundException e) {
            logger.error("Error localizing schema for {}", original.getClassName(), e);
            return original;
        }
    }

    private List<FieldInfo> collectAnnotatedFields(Class<?> clazz) {
        List<FieldInfo> fields = new ArrayList<>();

        // Walk up the class hierarchy
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                SchemaProperty schemaProp = field.getAnnotation(SchemaProperty.class);
                JsonProperty jsonProp = field.getAnnotation(JsonProperty.class);

                if (schemaProp != null || jsonProp != null) {
                    FieldInfo info = new FieldInfo();
                    info.field = field;
                    info.name = jsonProp != null ? jsonProp.value() : field.getName();
                    info.schemaProperty = schemaProp;
                    info.order = schemaProp != null ? schemaProp.order() : 100;
                    fields.add(info);
                }
            }
            current = current.getSuperclass();
        }

        return fields;
    }

    private Map<String, Object> buildJsonSchema(List<FieldInfo> fields, Locale locale) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");

        Map<String, Object> properties = new LinkedHashMap<>();

        for (FieldInfo info : fields) {
            Map<String, Object> prop = new LinkedHashMap<>();

            // Determine type
            String type = determineType(info);
            prop.put("type", type);

            // Add title and description
            String className = info.field.getDeclaringClass().getSimpleName();
            String fieldName = info.name;

            if (info.schemaProperty != null) {
                String titleKey = "rule." + className + ".field." + fieldName + ".title";
                String descKey = "rule." + className + ".field." + fieldName + ".description";

                String title = info.schemaProperty.title();
                String desc = info.schemaProperty.description();

                if (locale != null) {
                    title = messageSource.getMessage(titleKey, null, title, locale);
                    desc = messageSource.getMessage(descKey, null, desc, locale);
                }

                prop.put("title", title);
                if (!desc.isEmpty()) {
                    prop.put("description", desc);
                }
                if (!info.schemaProperty.defaultValue().isEmpty()) {
                    prop.put("default", parseDefault(info.schemaProperty.defaultValue(), type));
                }
            } else {
                prop.put("title", info.name);
            }

            // Handle array types
            if ("array".equals(type)) {
                handleArrayType(prop, info, locale);
            }

            properties.put(info.name, prop);
        }

        schema.put("properties", properties);
        return schema;
    }

    private void handleArrayType(Map<String, Object> prop, FieldInfo info, Locale locale) {
        Class<?> genericType = getListGenericType(info.field);

        if (genericType != null) {
            if (genericType == String.class) {
                Map<String, Object> items = new LinkedHashMap<>();
                items.put("type", "string");
                items.put("title", "valor");
                prop.put("items", items);
            } else {
                // Assume complex type, build schema for it
                Map<String, Object> items = new LinkedHashMap<>();
                items.put("type", "object");
                items.put("title", genericType.getSimpleName()); // Default title

                // Recursively build properties for the complex type
                List<FieldInfo> nestedFields = collectAnnotatedFields(genericType);
                Map<String, Object> nestedSchema = buildJsonSchema(nestedFields, locale);
                if (nestedSchema.containsKey("properties")) {
                    items.put("properties", nestedSchema.get("properties"));
                }

                prop.put("items", items);
            }
        } else {
            // Fallback for unknown generic type
            Map<String, Object> items = new LinkedHashMap<>();
            items.put("type", "string");
            items.put("title", "valor");
            prop.put("items", items);
        }
    }

    private Class<?> getListGenericType(Field field) {
        try {
            java.lang.reflect.Type genericType = field.getGenericType();
            if (genericType instanceof java.lang.reflect.ParameterizedType) {
                java.lang.reflect.ParameterizedType pt = (java.lang.reflect.ParameterizedType) genericType;
                java.lang.reflect.Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0) {
                    if (typeArgs[0] instanceof Class) {
                        return (Class<?>) typeArgs[0];
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not determine generic type for field {}", field.getName());
        }
        return null;
    }

    private List<Object> buildForm(List<FieldInfo> fields, String helpText, Locale locale) {
        List<Object> form = new ArrayList<>();

        // Add help text if present
        if (helpText != null && !helpText.isEmpty()) {
            Map<String, Object> help = new LinkedHashMap<>();
            help.put("type", "help");
            help.put("helpvalue", helpText);
            form.add(help);
        }

        // Add field references
        for (FieldInfo info : fields) {
            if (info.schemaProperty != null && !info.schemaProperty.uiType().isEmpty()) {
                // Custom UI type
                Map<String, Object> fieldDef = new LinkedHashMap<>();
                fieldDef.put("key", info.name);
                fieldDef.put("type", info.schemaProperty.uiType());
                form.add(fieldDef);
            } else {
                // Simple field reference
                form.add(info.name);
            }
        }

        // Add submit button
        Map<String, Object> submit = new LinkedHashMap<>();
        submit.put("type", "submit");
        String submitTitle = "Guardar regla";
        if (locale != null) {
            submitTitle = messageSource.getMessage("rule.common.submit", null, submitTitle, locale);
        }
        submit.put("title", submitTitle);
        form.add(submit);

        return form;
    }

    private String determineType(FieldInfo info) {
        // Check explicit type first
        if (info.schemaProperty != null && !info.schemaProperty.type().isEmpty()) {
            return info.schemaProperty.type();
        }

        // Infer from Java type
        Class<?> fieldType = info.field.getType();

        if (fieldType == Integer.class || fieldType == int.class ||
                fieldType == Long.class || fieldType == long.class) {
            return "integer";
        } else if (fieldType == Boolean.class || fieldType == boolean.class) {
            return "boolean";
        } else if (List.class.isAssignableFrom(fieldType)) {
            return "array";
        }

        return "string";
    }

    private Object parseDefault(String value, String type) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        switch (type) {
            case "integer":
                return Integer.parseInt(value);
            case "boolean":
                return Boolean.parseBoolean(value);
            default:
                return value;
        }
    }

    private static class FieldInfo {
        Field field;
        String name;
        SchemaProperty schemaProperty;
        int order;
    }
}
