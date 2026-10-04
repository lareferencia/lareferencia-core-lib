package org.lareferencia.core.task;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/** Initial legacy policy, matching V5.0.0.12 for catalogues created after migration. */
final class LegacyActionCatalogDefaults {
    private static final List<String> ORDER = List.of(
            "HARVESTING_ACTION", "DARK_STAGE_ACTION", "DARK_RECONCILE_ACTION", "VALIDATION_ACTION",
            "FRONTEND_INDEXING_ACTION", "XOAI_INDEXING_ACTION", "SEMANTIC_INDEXING_ACTION",
            "FRONTEND_DELETE_ACTION", "XOAI_DELETE_ACTION", "SEMANTIC_DELETE_ACTION",
            "METADATA_ORPHAN_CLEANUP_ACTION", "NETWORK_CLEAN_ACTION", "NETWORK_DELETE_ACTION");
    private static final Set<String> DISABLED = Set.of(
            "DARK_STAGE_ACTION", "DARK_RECONCILE_ACTION", "SEMANTIC_INDEXING_ACTION", "SEMANTIC_DELETE_ACTION");

    private LegacyActionCatalogDefaults() { }

    static Comparator<NetworkAction> order() {
        return Comparator.comparingInt((NetworkAction action) -> {
            int position = ORDER.indexOf(action.getName());
            return position < 0 ? Integer.MAX_VALUE : position;
        }).thenComparing(NetworkAction::getName);
    }

    static boolean enabled(String actionKey) {
        return ORDER.contains(actionKey) && !DISABLED.contains(actionKey);
    }
}
