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

package org.lareferencia.core.worker.validation.validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractValidatorRule;
import org.lareferencia.core.worker.validation.QuantifierValues;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;
import org.lareferencia.core.worker.validation.ValidatorRuleResult;

/**
 * Requires exactly one occurrence from the complete vocabulary, and requires
 * that term to be accepted by LA Referencia. Matching is exact, as in
 * ControlledValueFieldContentValidatorRule. Unrecognized values are ignored.
 * The inherited quantifier does not change this fixed cardinality policy.
 */
@Getter
@ValidatorRuleMeta
public class SingleAcceptedVocabularyValidatorRule extends AbstractValidatorRule {
    @SchemaProperty(order = 1)
    private final String fieldname;

    @SchemaProperty(type = "array", order = 2)
    private final List<VocabularyTerm> vocabulary;

    @JsonIgnore
    private final Map<String, Boolean> acceptedByValue;

    @JsonCreator
    public SingleAcceptedVocabularyValidatorRule(@JsonProperty("fieldname") String fieldname,
                                                @JsonProperty("vocabulary") List<VocabularyTerm> vocabulary) {
        if (fieldname == null || fieldname.isBlank()) {
            throw new IllegalArgumentException("A metadata field is required");
        }
        if (vocabulary == null || vocabulary.isEmpty()) {
            throw new IllegalArgumentException("The complete vocabulary is required");
        }
        Map<String, Boolean> terms = new HashMap<>();
        for (VocabularyTerm term : vocabulary) {
            if (term == null || terms.putIfAbsent(term.getValue(), term.isAccepted()) != null) {
                throw new IllegalArgumentException("Vocabulary terms must be nonnull and unique");
            }
        }
        if (!terms.containsValue(true)) {
            throw new IllegalArgumentException("At least one vocabulary term must be accepted");
        }
        this.fieldname = fieldname;
        this.vocabulary = List.copyOf(vocabulary);
        this.acceptedByValue = Map.copyOf(terms);
        this.quantifier = QuantifierValues.ONE_ONLY;
        this.storeOccurrences = true;
    }

    @Override
    public ValidatorRuleResult validate(OAIRecordMetadata metadata) {
        List<String> matches = new ArrayList<>();
        for (String value : metadata.getFieldOcurrences(fieldname)) {
            if (acceptedByValue.containsKey(value)) matches.add(value);
        }
        boolean valid = matches.size() == 1 && acceptedByValue.get(matches.get(0));
        List<ContentValidatorResult> details = new ArrayList<>();
        for (String value : matches) {
            details.add(new ContentValidatorResult(valid, value));
        }
        if (matches.isEmpty()) {
            details.add(new ContentValidatorResult(false, "no_vocabulary_occurrences_found"));
        }
        ValidatorRuleResult result = new ValidatorRuleResult();
        result.setRule(this);
        result.setResults(details);
        result.setValid(valid);
        return result;
    }
}
