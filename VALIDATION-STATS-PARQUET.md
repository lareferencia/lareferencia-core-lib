# Validation Statistics Parquet Configuration

## Overview

The fact table implementation completely replaces the previous architecture based in spring data solr. This new architecture normalizes data into a fact table with 1 row per validation rule occurrence.

## Configuration Properties

### Parquet Files Location

```properties
validation.stats.parquet.path=/tmp/validation-stats-parquet
```

**Description**: Base directory where partitioned Parquet files and metadata are stored, organized by network.

**Directory Structure** (updated to organize by network):
```
/tmp/validation-stats-parquet/
├── LA_REFERENCIA/                    # Network-level directory (sanitized acronym)
│   ├── metadata/                     # Metadata XML files for this network
│   │   ├── 0/
│   │   │   └── abc123def456.xml.gz
│   │   └── 1/
│   │       └── xyz789uvw012.xml.gz
│   └── snapshots/                    # Snapshot-specific data
│       ├── snapshot_8/
│       │   ├── metadata.json         # Snapshot metadata
│       │   ├── validation_stats.json # Validation statistics
│       │   ├── oai_records/          # OAI catalog records
│       │   │   └── records.parquet
│       │   └── validation_records/   # Validation fact table
│       │       ├── part-00000.parquet
│       │       └── part-00001.parquet
│       └── snapshot_9/
│           └── ...
└── OTHER_NETWORK/
    ├── metadata/
    └── snapshots/
```

**Key Changes**:
- **Network isolation**: Each network has its own top-level directory
- **Network acronym sanitization**: Spaces and special characters are converted (e.g., "LA Referencia" → "LA_REFERENCIA")
- **Metadata centralization**: XML metadata files are stored under `{NETWORK}/metadata/`
- **Snapshot organization**: All snapshot data is under `{NETWORK}/snapshots/snapshot_{id}/`

**Recommendations**:
- In production, use a distributed file system (HDFS, S3, Azure Blob)
- Ensure sufficient disk space
- Configure appropriate read/write permissions
- Network acronyms are automatically sanitized to uppercase alphanumeric + underscore/hyphen only

---

### Records Per File (Base)

```properties
parquet.validation.records-per-file=100000
```

**Description**: Base number of records per file when dynamic sizing is disabled or as fallback.

**Recommended Values**:
- **Development/Testing**: 1,000 - 10,000
- **Standard Production**: 100,000 - 500,000
- **Big Data**: 1,000,000 - 2,000,000

**Impact**:
- Very low values (< 10K): Too many small files, filesystem overhead
- Very high values (> 5M): Very large files, higher memory usage when reading

---

### Dynamic File Size Adjustment

```properties
parquet.validation.enable-dynamic-sizing=true
```

**Description**: Enables automatic file size adjustment based on snapshot size.

**Values**: `true` | `false`

**Dynamic Sizing Algorithm** (when enabled):

| Snapshot Size | Records/File | Expected Files | File Size     |
|---------------|--------------|----------------|---------------|
| < 100,000     | 50,000       | 1-2            | 1.5-4 MB      |
| 100K - 1M     | 500,000      | 2-20           | 15-40 MB      |
| 1M - 10M      | 1,000,000    | 1-10           | 30-80 MB      |
| > 10M         | 2,000,000    | 5+             | 60-160 MB     |

**Recommended**: `true` (automatically optimizes based on snapshot)

**Behavior**:
- Snapshot size is automatically registered during `initializeValidationForSnapshot()`
- Obtained from `NetworkSnapshot.size` via `IMetadataRecordStoreService.getSnapshotSize()`
- If size is not available, uses `parquet.validation.records-per-file` value

---

## Compression and Encoding Configuration

These values are hardcoded in `FactOccurrencesWriter` but can be externalized if needed:

### Compression

```java
CompressionCodecName.ZSTD  // Zstandard compression
```

**Characteristics**:
- Compression ratio: ~4:1 (better than Snappy)
- Read speed: ~450 MB/s
- Optimal balance between compression and speed

**Alternatives**:
- `SNAPPY`: Faster, less compression (ratio ~2:1)
- `GZIP`: Better compression (ratio ~6:1), slower
- `UNCOMPRESSED`: No compression (for debugging)

### Dictionary Encoding

```java
.withDictionaryEncoding(true)
```

**Description**: Enables dictionary encoding for repetitive fields.

**Benefits**:
- Reduces size of fields with repeated values (network, repository, institution)
- Improves query speed with filters on these fields
- ~30-50% additional size reduction

### Row Group Size

```java
.withRowGroupSize(128 * 1024 * 1024)  // 128 MB
```

**Description**: Row group size to optimize sequential reading.

**Recommended Values**:
- **HDFS/S3**: 128 MB - 256 MB (optimal for HDFS blocks)
- **Local disk**: 64 MB - 128 MB
- **Limited memory**: 32 MB - 64 MB

### Page Size

```java
.withPageSize(1024 * 1024)  // 1 MB
```

**Description**: Page size for balance between compression and seek efficiency.

**Typical Values**:
- **Standard**: 1 MB (optimal balance)
- **Frequent queries**: 512 KB (better seek)
- **Full scan**: 2 MB (better compression)

---

## Parquet/Hadoop Logging

To reduce log noise:

```properties
logging.level.org.apache.parquet.hadoop.InternalParquetRecordReader=WARN
logging.level.org.apache.hadoop.io.compress.CodecPool=WARN
logging.level.org.apache.parquet=WARN
logging.level.org.apache.hadoop=WARN
```

For detailed debugging:

```properties
logging.level.org.lareferencia.backend.repositories.parquet=DEBUG
logging.level.org.lareferencia.backend.validation=DEBUG
```

---

## Fact Table Schema

### Fields (14 columns)

| Field             | Type    | Required | Description                                  |
|-------------------|---------|----------|----------------------------------------------|
| id                | STRING  | Yes      | Unique record ID (MD5/XXHash)                |
| identifier        | STRING  | Yes      | OAI identifier                               |
| snapshot_id       | LONG    | Yes      | Snapshot ID (partition)                      |
| origin            | STRING  | Yes      | Origin URL                                   |
| network           | STRING  | No       | Network acronym (partition)                  |
| repository        | STRING  | No       | Repository name                              |
| institution       | STRING  | No       | Institution name                             |
| rule_id           | INT     | Yes      | Validation rule ID                           |
| value             | STRING  | No       | Occurrence value                             |
| is_valid          | BOOLEAN | Yes      | Valid occurrence (partition)                 |
| record_is_valid   | BOOLEAN | Yes      | Complete record valid                        |
| is_transformed    | BOOLEAN | Yes      | Record transformed                           |
| metadata_prefix   | STRING  | No       | Metadata prefix (e.g., "xoai")               |
| set_spec          | STRING  | No       | OAI set specification                        |

### Partitioning

**Strategy**: Network-based directory structure with file-based organization

**Directory Organization**:
```
{basePath}/
└── {NETWORK_ACRONYM}/              # Top-level: sanitized network acronym
    ├── metadata/                    # XML metadata for this network
    │   └── {hash_prefix}/
    │       └── {hash}.xml.gz
    └── snapshots/
        └── snapshot_{id}/           # Snapshot-specific data
            ├── metadata.json        # Snapshot metadata
            ├── validation_stats.json # Validation statistics summary
            ├── oai_records/         # OAI catalog records (Parquet)
            └── validation_records/  # Validation fact table (Parquet)
```

**Advantages**:
- **Network isolation**: Each network has dedicated storage space
- **Simplified management**: Easy to backup/restore per network
- **Clear organization**: Metadata and snapshots clearly separated
- **Filesystem-level filtering**: OS-level operations on specific networks
- **Scalability**: Networks can be distributed across different storage systems

**Path Construction** (handled by `PathUtils`):
- Metadata: `{basePath}/{NETWORK}/metadata/{hash[0]}/{hash}.xml.gz`
- Snapshots base: `{basePath}/{NETWORK}/snapshots/`
- Snapshot data: `{basePath}/{NETWORK}/snapshots/snapshot_{id}/`

---

## Migration from Previous Version

### Migration Steps (v4.x to v5.0)

1. **Backup existing data** (optional):
   ```bash
   mv /tmp/validation-stats-parquet /tmp/validation-stats-parquet.old
   ```

2. **Clean directories**:
   ```bash
   rm -rf /tmp/validation-stats-parquet
   mkdir -p /tmp/validation-stats-parquet
   ```

3. **Update code**: Replace all deprecated method calls with `SnapshotMetadata`-based API (see API Changes section)

4. **Update configuration**: Use recommended values in `application.properties`

5. **Restart application**: Data will be regenerated on next validation with new directory structure

6. **Verify structure**:
   ```bash
   # Check network-based organization
   ls -la /tmp/validation-stats-parquet/
   
   # Check specific network
   ls -la /tmp/validation-stats-parquet/LA_REFERENCIA/snapshots/
   
   # Check snapshot data
   ls -la /tmp/validation-stats-parquet/LA_REFERENCIA/snapshots/snapshot_*/
   ```

### Key Changes

**Directory Structure**:
- Old: `snapshot_id={id}/network={acronym}/is_valid={true|false}/`
- New: `{NETWORK}/snapshots/snapshot_{id}/validation_records/`

**API**:
- All methods now require `SnapshotMetadata` instead of `Long snapshotId`
- Deprecated methods have been removed

**Compatibility**:
- **No backward compatibility** with old directory structure
- Parquet files must be completely regenerated
- Code must be updated to use new API

---

## Performance Tuning

### For small snapshots (< 100K)

```properties
parquet.validation.enable-dynamic-sizing=true
parquet.validation.records-per-file=50000
```

### For large snapshots (> 10M)

```properties
parquet.validation.enable-dynamic-sizing=true
parquet.validation.records-per-file=2000000
```

### For systems with limited memory

Reduce row group size in code:
```java
.withRowGroupSize(64 * 1024 * 1024)  // 64 MB instead of 128 MB
```

### To improve write speed

Reduce compression (in code):
```java
.withCompressionCodec(CompressionCodecName.SNAPPY)  // Instead of ZSTD
```

---

## Troubleshooting

### Problem: Too many small files

**Cause**: `records-per-file` too low or dynamic sizing disabled

**Solution**:
```properties
parquet.validation.enable-dynamic-sizing=true
parquet.validation.records-per-file=100000  # Increase
```

### Problem: OutOfMemoryError when reading

**Cause**: Row group size too large or queries without filters

**Solution**:
1. Reduce row group size to 64 MB
2. Always use filters by snapshot_id
3. Increase heap: `-Xmx4g`

### Problem: Filters not working

**Verify**:
1. `BUILD PREDICATE` logs show the constructed predicate
2. Use compatible filters: `isValid`, `valid_rules`, `invalid_rules`, `identifier`
3. Correct format: `field:value` or `field@@"value"`

### Problem: Slow performance

**Diagnose**:
```properties
logging.level.org.lareferencia.backend.repositories.parquet=DEBUG
```

**Optimize**:
1. Verify predicate pushdown is active (logs show filters)
2. Use partitioning: Always filter by `snapshot_id`
3. Consider Parquet indexes if available in future version

---

## Monitoring

### Key Metrics

- **Number of files per snapshot**: Ideally < 20 files per partition
- **Average file size**: 30-80 MB is optimal
- **Records read**: Logs show `FactOccurrencesReader: Records read: X`
- **Records written**: Logs show `FactOccurrencesWriter: Records written: X`

### Useful Commands

**Count files**:
```bash
find /tmp/validation-stats-parquet -name "*.parquet" | wc -l
```

**Total size**:
```bash
du -sh /tmp/validation-stats-parquet
```

**Files per snapshot**:
```bash
ls -la /tmp/validation-stats-parquet/LA_REFERENCIA/snapshots/snapshot_8/*/*.parquet
```

---

## API Changes (v5.0)

### SnapshotMetadata-Based API

All Parquet repository methods now require a `SnapshotMetadata` object instead of just a `Long snapshotId`. This ensures proper network isolation and path construction.

**Old API (deprecated, removed in v5.0)**:
```java
// ❌ No longer supported
oaiRecordRepository.initializeSnapshot(snapshotId);
validationStatRepository.getSnapshotValidationStats(snapshotId);
```

**New API (required)**:
```java
// ✅ Required: Pass complete SnapshotMetadata
SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotId);
oaiRecordRepository.initializeSnapshot(metadata);
validationStatRepository.getSnapshotValidationStats(metadata);
```

### Updated Methods

#### OAIRecordParquetRepository
- `initializeSnapshot(SnapshotMetadata)` - Initialize snapshot for OAI record writing
- `readAllRecords(SnapshotMetadata)` - Read all records (not recommended for large datasets)
- `iterateRecords(SnapshotMetadata)` - Lazy iteration for large datasets (recommended)
- `countRecords(SnapshotMetadata)` - Count records without loading

#### ValidationStatParquetRepository
- `getSnapshotValidationStats(SnapshotMetadata)` - Get validation statistics

#### Path Utilities
- `PathUtils.sanitizeNetworkAcronym(String)` - Sanitize network acronym for filesystem
- `PathUtils.getMetadataStorePath(String, SnapshotMetadata)` - Get metadata XML path
- `PathUtils.getSnapshotsBasePath(String, SnapshotMetadata)` - Get snapshots base path
- `PathUtils.getSnapshotPath(String, SnapshotMetadata)` - Get specific snapshot path

### Network Acronym Sanitization

Network acronyms are automatically sanitized for filesystem compatibility:
- Converted to uppercase
- Only alphanumeric characters, hyphens, and underscores allowed
- Spaces and special characters replaced with underscores

**Examples**:
```java
"LA Referencia" → "LA_REFERENCIA"
"test-network" → "TEST-NETWORK"
"México 2024" → "M_XICO_2024"
```

### Migration Guide

1. **Update all method calls** to pass `SnapshotMetadata`:
   ```java
   // Before
   Long snapshotId = ...;
   repository.method(snapshotId);
   
   // After
   SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotId);
   repository.method(metadata);
   ```

2. **Ensure SnapshotMetadata has networkAcronym**:
   ```java
   metadata.setNetworkAcronym(network.getAcronym());
   ```

3. **Update tests** to provide `networkAcronym` in test metadata:
   ```java
   SnapshotMetadata testMetadata = new SnapshotMetadata(1L);
   testMetadata.setNetworkAcronym("TEST");
   ```

---

## References

- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [Parquet File Format Specification](https://github.com/apache/parquet-format)
- [Hadoop Configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml)
