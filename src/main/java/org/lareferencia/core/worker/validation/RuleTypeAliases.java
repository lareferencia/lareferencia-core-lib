package org.lareferencia.core.worker.validation;

import org.lareferencia.core.worker.validation.transformer.FieldContentNormalizeRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Resolve the historical core rule packages in type discriminators only. */
public final class RuleTypeAliases {
    private static final String LEGACY = "org.lareferencia.backend.validation.";
    private static final String CURRENT = "org.lareferencia.core.worker.validation.";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RuleTypeAliases() {}

    public static String canonicalName(String name) {
        if ((CURRENT + "transformer.SQLiteEnrichmentRule").equals(name)) {
            return CURRENT + "transformer.ImportEnrichments";
        }
        if (name == null || !(name.startsWith(LEGACY + "validator.")
                || name.startsWith(LEGACY + "transformer."))) return name;
        String candidate = CURRENT + name.substring(LEGACY.length());
        try {
            Class<?> type = Class.forName(candidate, false, RuleTypeAliases.class.getClassLoader());
            if (IValidatorRule.class.isAssignableFrom(type) || ITransformerRule.class.isAssignableFrom(type)) {
                return candidate;
            }
        } catch (ClassNotFoundException ignored) {
            // Extensions with their own type name continue through normal resolution.
        }
        return name;
    }

    public static String resolve(String json) throws JsonProcessingException {
        if (json == null) return null;
        JsonNode root = MAPPER.readTree(json);
        return normalize(root) ? MAPPER.writeValueAsString(root) : json;
    }

    private static boolean normalize(JsonNode node) {
        boolean changed = false;
        if (node instanceof ObjectNode object && object.path("@class").isTextual()) {
            String name = object.get("@class").textValue();
            String canonical = canonicalName(name);
            if (!name.equals(canonical)) {
                object.put("@class", canonical);
                changed = true;
            }
        }
        // validationRule is the nested polymorphic property of FieldContentNormalizeRule.
        // Dictionary entries and other administrator-provided objects contain literal data.
        if (FieldContentNormalizeRule.class.getName()
                .equals(node.path("@class").asText()) && node.path("validationRule").isObject()) {
            changed |= normalize(node.get("validationRule"));
        }
        return changed;
    }
}
