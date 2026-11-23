package org.lareferencia.core.service.validation;

import java.util.List;

/** Distinct metadata values on this page, and totals for the full filtered group. */
public record FrequencyPage(List<OccurrenceCount> content, long totalElements, long totalOccurrences) {}
