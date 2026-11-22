BEGIN TRANSACTION;
CREATE TABLE items (
    id INTEGER PRIMARY KEY, catalog_id TEXT NOT NULL UNIQUE, identifier TEXT NOT NULL, metadata_hash TEXT NOT NULL
);
INSERT INTO "items" VALUES(1,'stored-id','source-id','hash');
CREATE TABLE occurrences (
    id INTEGER PRIMARY KEY, target TEXT NOT NULL, content TEXT NOT NULL, UNIQUE(target, content)
);
INSERT INTO "occurrences" VALUES(1,'dc.rights.license_l2','{"jurisdiction":"BR","value":"CC"}');
INSERT INTO "occurrences" VALUES(2,'dc.rights.license_l2','{"jurisdiction":"BR","value":"BY"}');
INSERT INTO "occurrences" VALUES(3,'dc.rights.license_l2','{"jurisdiction":"BR","value":"X"}');
CREATE TABLE package (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL, network TEXT NOT NULL, snapshot TEXT NOT NULL, variant TEXT NOT NULL
);
INSERT INTO "package" VALUES(1,1,'PY','1','original');
CREATE TABLE result_occurrences (
    result INTEGER NOT NULL REFERENCES results, position INTEGER NOT NULL, occurrence INTEGER NOT NULL REFERENCES occurrences,
    PRIMARY KEY(result, position)
) WITHOUT ROWID;
INSERT INTO "result_occurrences" VALUES(1,0,1);
INSERT INTO "result_occurrences" VALUES(1,1,2);
INSERT INTO "result_occurrences" VALUES(2,0,1);
INSERT INTO "result_occurrences" VALUES(2,1,3);
CREATE TABLE results (
    id INTEGER PRIMARY KEY, item INTEGER NOT NULL REFERENCES items, producer TEXT NOT NULL,
    run INTEGER NOT NULL REFERENCES runs, generated_at TEXT NOT NULL, source_ref TEXT,
    UNIQUE(item, producer)
);
INSERT INTO "results" VALUES(1,1,'rights',1,'2026-01-01T00:00:00Z',NULL);
INSERT INTO "results" VALUES(2,1,'other',2,'2026-01-01T00:00:00Z',NULL);
CREATE TABLE runs (
    id INTEGER PRIMARY KEY, producer TEXT NOT NULL, run_key TEXT NOT NULL, metadata TEXT NOT NULL,
    UNIQUE(producer, run_key)
);
INSERT INTO "runs" VALUES(1,'rights','1','{"exportId":"exp-demo","network":"PY","producer":"rights","runId":"1","schemaVersion":"1","snapshot":"1","type":"facet","variant":"original"}');
INSERT INTO "runs" VALUES(2,'other','1','{"exportId":"exp-demo","network":"PY","producer":"other","runId":"1","schemaVersion":"1","snapshot":"1","type":"facet","variant":"original"}');
CREATE INDEX items_identifier ON items(identifier);
COMMIT;
PRAGMA user_version=1;
