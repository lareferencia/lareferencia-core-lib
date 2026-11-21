/*
 *   Copyright (c) 2013-2026. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 */

package org.lareferencia.core.service.validation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.Transformer;
import org.lareferencia.core.domain.TransformerRule;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.metadata.SnapshotMetadata.NetworkInfo;
import org.lareferencia.core.worker.validation.RuleTypeAliases;
import org.lareferencia.core.worker.validation.transformer.FieldContentNormalizeRule;
import org.lareferencia.core.worker.validation.validator.DynamicYearRangeFieldContentValidatorRule;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Creates a stable SHA-256 fingerprint for the effective validator definition.
 * JSON object member order and whitespace are deliberately ignored. Array order
 * remains significant because it is part of a rule definition's JSON value.
 */
@Service
public class ValidatorFingerprintService {

    static final int FORMAT_VERSION = 1;
    static final String ALGORITHM = "SHA-256";
    static final String CANONICALIZER = "validator-v1";
    private static final String VALIDATION_ENGINE_VERSION = "validator-engine-v2";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ValidatorFingerprint fingerprint(Validator validator) {
        return fingerprint(validator, Year.now().getValue());
    }

    private ValidatorFingerprint fingerprint(Validator validator, int referenceYear) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("fingerprintVersion", FORMAT_VERSION);
        root.put("validationEngineVersion", VALIDATION_ENGINE_VERSION);

        if (validator == null) {
            root.put("validatorMode", "none");
            root.putArray("rules");
        } else {
            root.put("validatorMode", "configured");
            ArrayNode rules = root.putArray("rules");
            List<String> normalizedRules = new ArrayList<>();
            for (ValidatorRule rule : validator.getRules()) {
                normalizedRules.add(canonicalRule(rule, referenceYear));
            }
            normalizedRules.sort(Comparator.naturalOrder());
            for (String normalizedRule : normalizedRules) {
                try {
                    rules.add(objectMapper.readTree(normalizedRule));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException("Cannot rebuild canonical validator rule", e);
                }
            }
        }

        String canonical = writeCanonical(root);
        return new ValidatorFingerprint(FORMAT_VERSION, ALGORITHM, CANONICALIZER, sha256(canonical));
    }

    /** Fingerprint every input used before validation, including ordered transformations. */
    public ValidatorFingerprint fingerprintNetwork(Network network) {
        return fingerprintNetwork(network, false);
    }

    public ValidatorFingerprint fingerprintNetwork(Network network, boolean detailedDiagnose) {
        return fingerprintNetwork(network, detailedDiagnose, Year.now().getValue());
    }

    public ValidatorFingerprint fingerprintNetwork(Network network, boolean detailedDiagnose,
                                                   int referenceYear) {
        ObjectNode pipeline = objectMapper.createObjectNode();
        pipeline.put("pipelineVersion", 2);
        pipeline.put("detailedDiagnose", detailedDiagnose);
        pipeline.put("validator", fingerprint(network.getValidator(), referenceYear).getHash());
        pipeline.set("primary", canonicalTransformer(network.getTransformer(), referenceYear));
        pipeline.set("secondary", canonicalTransformer(network.getSecondaryTransformer(), referenceYear));
        pipeline.set("network", sortObjectFields(objectMapper.valueToTree(
                new NetworkInfo(network))));
        return new ValidatorFingerprint(2, ALGORITHM, "validation-pipeline-v2", sha256(writeCanonical(pipeline)));
    }

    private JsonNode canonicalTransformer(Transformer transformer, int referenceYear) {
        ArrayNode rules = objectMapper.createArrayNode();
        if (transformer != null) {
            transformer.getRules().stream()
                    .sorted(Comparator.comparing(TransformerRule::getRunorder))
                    .forEach(rule -> rules.add(canonicalizeJson(rule.getJsonserialization(), referenceYear)));
        }
        return rules;
    }

    private String canonicalRule(ValidatorRule rule, int referenceYear) {
        ObjectNode normalized = objectMapper.createObjectNode();
        if (rule.getId() == null) {
            normalized.putNull("id");
        } else {
            normalized.put("id", rule.getId());
        }
        normalized.put("mandatory", Boolean.TRUE.equals(rule.getMandatory()));
        if (rule.getQuantifier() == null) {
            normalized.putNull("quantifier");
        } else {
            normalized.put("quantifier", rule.getQuantifier().name());
        }
        normalized.set("definition", canonicalizeJson(rule.getJsonserialization(), referenceYear));
        return writeCanonical(normalized);
    }

    private JsonNode canonicalizeJson(String json, int referenceYear) {
        if (json == null) {
            return objectMapper.nullNode();
        }
        try {
            JsonNode definition = objectMapper.readTree(json);
            String type = RuleTypeAliases.canonicalName(definition.path("@class").asText());
            if (FieldContentNormalizeRule.class.getName().equals(type)) {
                type = RuleTypeAliases.canonicalName(definition.path("validationRule").path("@class").asText());
            }
            if (DynamicYearRangeFieldContentValidatorRule.class.getName().equals(type)) {
                ObjectNode dated = objectMapper.createObjectNode();
                dated.put("referenceYear", referenceYear);
                dated.set("definition", definition);
                return sortObjectFields(dated);
            }
            return sortObjectFields(definition);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Validator rule contains invalid JSON", e);
        }
    }

    private JsonNode sortObjectFields(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = objectMapper.createObjectNode();
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(Comparator.naturalOrder());
            for (String name : names) {
                sorted.set(name, sortObjectFields(node.get(name)));
            }
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode sorted = objectMapper.createArrayNode();
            for (JsonNode child : node) {
                sorted.add(sortObjectFields(child));
            }
            return sorted;
        }
        return node;
    }

    private String writeCanonical(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize validator fingerprint input", e);
        }
    }

    private String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance(ALGORITHM).digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(ALGORITHM + " is not available", e);
        }
    }
}
