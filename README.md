# LA Referencia Core Library

Shared library for the LA Referencia platform: OAI-PMH harvesting, metadata
processing, catalog and validation persistence, statistics, workers and the
workflow/action engine (verified against `src/main/java/org/lareferencia/core/`, 2026-09-23).

## 🎯 Packages (`org.lareferencia.core`)

| Package | Purpose |
|---|---|
| `domain` | Entities and value objects |
| `metadata` | Metadata stores (FS/H2/SQLite) and snapshot handling |
| `repository.catalog` | SQLite OAI catalog (`oai_record`, incremental `change_type` `N`/`U`/`D`) |
| `repository.validation` | SQLite validation persistence (`validation.db`, rules, occurrences) |
| `service` | Metadata, validation, statistics and indexing services |
| `worker` | Harvesting, validation, indexing and cleaning workers |
| `task` | Actions, scheduling and the legacy workflow engine |
| `flowable` | Optional BPMN (Flowable) engine integration |
| `util` | Cross-cutting utilities (`ConfigPathResolver`, …) |
| `embedding` | Embedding support |
| `oabroker` | OA Broker integration |

Conventions: the current namespace is `org.lareferencia.core` (never the legacy
`org.lareferencia.backend`). See
[`docs/REFACTORING_PACKAGE_STRUCTURE.md`](../docs/REFACTORING_PACKAGE_STRUCTURE.md).

Key behaviors:

- **Incremental processing** (since 2026-09-04): catalog rows carry
  `change_type` (`N`/`U`/`D`), `streamChanged()` iterates changed records, and
  validation databases are reused across snapshots via validator fingerprint +
  manifest. See [`docs/ISSUE_INCREMENTAL_RECORD_PROCESSING.md`](../docs/ISSUE_INCREMENTAL_RECORD_PROCESSING.md).
- **Configuration directory** resolved through `ConfigPathResolver`
  (`app.config.dir`, default `config`; see [`docs/CONFIG_DIRECTORY.md`](../docs/CONFIG_DIRECTORY.md)).
- **Workflow**: `workflow.engine=legacy` (TaskManager) or `flowable` (BPMN);
  action configuration tables consolidated in `V5.0.0.8__Harvester_action_configuration.sql`
  (see [`docs/WORKFLOW_ACTIONS.md`](../docs/WORKFLOW_ACTIONS.md)).

## 📄 License

Licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.  
See [LICENSE.txt](../LICENSE.txt) for complete terms.

## 📧 Support

**Email**: soporte@lareferencia.redclara.net

---

**LA Referencia** - Red Latinoamericana y de España de Ciencia Abierta  
Part of the LA Referencia Platform 5.0.0-rc2
