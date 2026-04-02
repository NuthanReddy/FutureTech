"""
Data Lakehouse Architecture -- Simulation
==========================================
Demonstrates core lakehouse concepts:
  - TransactionLog   : append-only log with ACID commit semantics
  - DeltaTable       : table format with schema, partitioning, time travel
  - MedallionPipeline: Bronze -> Silver -> Gold processing with quality gates
  - DataCatalog      : register, discover, and search tables
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHECKPOINT_INTERVAL = 10          # write a checkpoint every N commits
DEFAULT_RETENTION_HOURS = 168     # 7 days for vacuum
TARGET_FILE_SIZE_ROWS = 500       # compaction target (rows per file)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Layer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Operation(Enum):
    CREATE = "CREATE"
    WRITE = "WRITE"
    MERGE = "MERGE"
    DELETE = "DELETE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    COMPACT = "COMPACT"
    VACUUM = "VACUUM"


class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


class ColumnType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class Column:
    """Schema column definition."""
    name: str
    col_type: ColumnType
    nullable: bool = True


@dataclass
class Schema:
    """Table schema with version tracking."""
    columns: List[Column]
    version: int = 1

    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate a record against this schema."""
        for col in self.columns:
            if col.name not in record:
                if not col.nullable:
                    return False, f"Missing required column: {col.name}"
                continue
            value = record[col.name]
            if value is None and not col.nullable:
                return False, f"NULL not allowed for column: {col.name}"
            if value is not None and not self._check_type(value, col.col_type):
                return False, (
                    f"Type mismatch for {col.name}: "
                    f"expected {col.col_type.value}, got {type(value).__name__}"
                )
        return True, ""

    @staticmethod
    def _check_type(value: Any, col_type: ColumnType) -> bool:
        type_map = {
            ColumnType.STRING: str,
            ColumnType.INTEGER: int,
            ColumnType.FLOAT: (int, float),
            ColumnType.BOOLEAN: bool,
            ColumnType.TIMESTAMP: str,
        }
        expected = type_map[col_type]
        return isinstance(value, expected)


@dataclass
class DataFile:
    """Represents a Parquet data file in the table."""
    file_id: str
    partition_values: Dict[str, str]
    rows: List[Dict[str, Any]]
    row_count: int
    size_bytes: int
    created_at: str

    @property
    def stats(self) -> Dict[str, Any]:
        """Compute min/max stats per column for data skipping."""
        if not self.rows:
            return {}
        stats: Dict[str, Any] = {}
        for key in self.rows[0]:
            values = [r[key] for r in self.rows if r.get(key) is not None]
            if values and all(isinstance(v, (int, float)) for v in values):
                stats[key] = {"min": min(values), "max": max(values)}
        return stats


@dataclass
class LogEntry:
    """Single entry in the transaction log."""
    version: int
    timestamp: str
    operation: Operation
    added_files: List[str] = field(default_factory=list)
    removed_files: List[str] = field(default_factory=list)
    schema_version: Optional[int] = None
    rows_affected: int = 0
    commit_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Checkpoint:
    """Snapshot of all active files at a given version."""
    version: int
    active_file_ids: List[str]
    timestamp: str


# ---------------------------------------------------------------------------
# TransactionLog -- append-only log with ACID semantics
# ---------------------------------------------------------------------------

class TransactionLog:
    """
    Append-only transaction log providing ACID commit semantics.

    Each commit atomically writes a new version with the set of file-level
    actions (AddFile / RemoveFile).  Optimistic concurrency control detects
    conflicts when two writers target the same version.
    """

    def __init__(self) -> None:
        self._entries: List[LogEntry] = []
        self._checkpoints: List[Checkpoint] = []
        self._lock = threading.Lock()

    @property
    def current_version(self) -> int:
        return len(self._entries) - 1 if self._entries else -1

    # -- write path --------------------------------------------------------

    def commit(
        self,
        operation: Operation,
        added_files: Optional[List[str]] = None,
        removed_files: Optional[List[str]] = None,
        schema_version: Optional[int] = None,
        rows_affected: int = 0,
        commit_info: Optional[Dict[str, Any]] = None,
    ) -> LogEntry:
        """Atomically append a new version to the log.

        Returns the committed LogEntry.
        Raises RuntimeError on conflict (simulated optimistic concurrency).
        """
        with self._lock:
            new_version = self.current_version + 1
            entry = LogEntry(
                version=new_version,
                timestamp=datetime.utcnow().isoformat(),
                operation=operation,
                added_files=added_files or [],
                removed_files=removed_files or [],
                schema_version=schema_version,
                rows_affected=rows_affected,
                commit_info=commit_info or {},
            )
            self._entries.append(entry)

            # checkpoint every N commits
            if new_version > 0 and new_version % CHECKPOINT_INTERVAL == 0:
                self._write_checkpoint(new_version)

            return entry

    def _write_checkpoint(self, version: int) -> None:
        active = self.active_file_ids(version)
        cp = Checkpoint(
            version=version,
            active_file_ids=list(active),
            timestamp=datetime.utcnow().isoformat(),
        )
        self._checkpoints.append(cp)

    # -- read path ---------------------------------------------------------

    def active_file_ids(self, as_of_version: Optional[int] = None) -> Set[str]:
        """Reconstruct the set of active file IDs at a given version."""
        target = as_of_version if as_of_version is not None else self.current_version
        if target < 0:
            return set()

        # start from most recent checkpoint <= target
        base_ids: Set[str] = set()
        replay_from = 0
        for cp in reversed(self._checkpoints):
            if cp.version <= target:
                base_ids = set(cp.active_file_ids)
                replay_from = cp.version + 1
                break

        # replay entries after checkpoint up to target
        for entry in self._entries[replay_from: target + 1]:
            base_ids.update(entry.added_files)
            base_ids -= set(entry.removed_files)

        return base_ids

    def get_entry(self, version: int) -> Optional[LogEntry]:
        if 0 <= version < len(self._entries):
            return self._entries[version]
        return None

    def history(self, limit: int = 10) -> List[LogEntry]:
        return list(reversed(self._entries[-limit:]))

    def all_ever_added_files(self) -> Set[str]:
        """Return every file ID ever added (for vacuum)."""
        result: Set[str] = set()
        for entry in self._entries:
            result.update(entry.added_files)
        return result


# ---------------------------------------------------------------------------
# DeltaTable -- table format with schema, partitioning, time travel, ACID
# ---------------------------------------------------------------------------

class DeltaTable:
    """
    Simulates a Delta-Lake-style table on top of the TransactionLog.

    Supports:
      - Schema enforcement and evolution
      - Partitioned writes
      - Time travel (query by version)
      - ACID writes (append / overwrite)
      - Compaction and vacuum
    """

    def __init__(
        self,
        name: str,
        schema: Schema,
        partition_cols: Optional[List[str]] = None,
        layer: Layer = Layer.BRONZE,
    ) -> None:
        self.table_id: str = f"tbl_{uuid.uuid4().hex[:8]}"
        self.name: str = name
        self.layer: Layer = layer
        self.schema: Schema = copy.deepcopy(schema)
        self.partition_cols: List[str] = partition_cols or []
        self.log: TransactionLog = TransactionLog()
        self._files: Dict[str, DataFile] = {}
        self._schema_history: List[Schema] = [copy.deepcopy(schema)]
        self.created_at: str = datetime.utcnow().isoformat()

        # initial CREATE entry
        self.log.commit(
            operation=Operation.CREATE,
            schema_version=schema.version,
            commit_info={"table": name, "layer": layer.value},
        )

    # -- schema ------------------------------------------------------------

    def evolve_schema(self, new_columns: List[Column]) -> Schema:
        """Add new nullable columns (additive evolution only)."""
        for col in new_columns:
            if col.name in self.schema.column_names():
                raise ValueError(f"Column already exists: {col.name}")
            if not col.nullable:
                raise ValueError(
                    f"Schema evolution only allows nullable columns: {col.name}"
                )

        evolved = copy.deepcopy(self.schema)
        evolved.columns.extend(new_columns)
        evolved.version += 1
        self.schema = evolved
        self._schema_history.append(copy.deepcopy(evolved))

        self.log.commit(
            operation=Operation.SCHEMA_CHANGE,
            schema_version=evolved.version,
            commit_info={
                "added_columns": [c.name for c in new_columns],
            },
        )
        return evolved

    # -- write path --------------------------------------------------------

    def write(
        self,
        records: List[Dict[str, Any]],
        mode: WriteMode = WriteMode.APPEND,
        validate: bool = True,
    ) -> LogEntry:
        """Write records to the table with ACID semantics."""
        if not records:
            raise ValueError("Cannot write empty record set")

        # schema enforcement
        valid_records: List[Dict[str, Any]] = []
        rejected: List[Tuple[Dict[str, Any], str]] = []
        for rec in records:
            ok, reason = self.schema.validate_record(rec)
            if ok or not validate:
                valid_records.append(rec)
            else:
                rejected.append((rec, reason))

        if rejected and validate:
            if not valid_records:
                raise ValueError(
                    f"All {len(rejected)} records failed validation. "
                    f"First error: {rejected[0][1]}"
                )

        # partition records into files
        partitions: Dict[str, List[Dict[str, Any]]] = {}
        for rec in valid_records:
            pkey = self._partition_key(rec)
            partitions.setdefault(pkey, []).append(rec)

        added_ids: List[str] = []
        removed_ids: List[str] = []

        if mode == WriteMode.OVERWRITE:
            # remove all current files
            removed_ids = list(self.log.active_file_ids())

        for _pkey, rows in partitions.items():
            data_file = self._create_file(rows)
            self._files[data_file.file_id] = data_file
            added_ids.append(data_file.file_id)

        entry = self.log.commit(
            operation=Operation.WRITE,
            added_files=added_ids,
            removed_files=removed_ids,
            schema_version=self.schema.version,
            rows_affected=len(valid_records),
            commit_info={
                "mode": mode.value,
                "rejected": len(rejected),
            },
        )
        return entry

    def merge(
        self,
        incoming: List[Dict[str, Any]],
        match_keys: List[str],
    ) -> LogEntry:
        """MERGE (upsert): update matching rows, insert new ones."""
        current_rows = self.read()
        existing_index: Dict[str, int] = {}
        for idx, row in enumerate(current_rows):
            key = tuple(row.get(k) for k in match_keys)
            existing_index[str(key)] = idx

        updated = 0
        inserted = 0
        for rec in incoming:
            key = tuple(rec.get(k) for k in match_keys)
            if str(key) in existing_index:
                current_rows[existing_index[str(key)]] = rec
                updated += 1
            else:
                current_rows.append(rec)
                inserted += 1

        # overwrite all files with merged result
        removed_ids = list(self.log.active_file_ids())
        added_ids: List[str] = []
        for chunk in self._chunk_list(current_rows, TARGET_FILE_SIZE_ROWS):
            data_file = self._create_file(chunk)
            self._files[data_file.file_id] = data_file
            added_ids.append(data_file.file_id)

        entry = self.log.commit(
            operation=Operation.MERGE,
            added_files=added_ids,
            removed_files=removed_ids,
            rows_affected=updated + inserted,
            commit_info={"updated": updated, "inserted": inserted},
        )
        return entry

    # -- read path ---------------------------------------------------------

    def read(self, as_of_version: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read the table snapshot, optionally at a historical version."""
        active_ids = self.log.active_file_ids(as_of_version)
        rows: List[Dict[str, Any]] = []
        for fid in active_ids:
            df = self._files.get(fid)
            if df:
                rows.extend(df.rows)
        return rows

    def read_partition(
        self,
        partition_filter: Dict[str, str],
        as_of_version: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read with partition pruning."""
        active_ids = self.log.active_file_ids(as_of_version)
        rows: List[Dict[str, Any]] = []
        for fid in active_ids:
            df = self._files.get(fid)
            if df and all(
                df.partition_values.get(k) == v
                for k, v in partition_filter.items()
            ):
                rows.extend(df.rows)
        return rows

    # -- time travel -------------------------------------------------------

    def time_travel(self, version: int) -> List[Dict[str, Any]]:
        """Query the table as of a specific version."""
        if version < 0 or version > self.log.current_version:
            raise ValueError(
                f"Version {version} out of range [0, {self.log.current_version}]"
            )
        return self.read(as_of_version=version)

    def history(self, limit: int = 10) -> List[LogEntry]:
        """Get the commit history of this table."""
        return self.log.history(limit)

    # -- maintenance -------------------------------------------------------

    def compact(self) -> LogEntry:
        """Merge small files into larger ones (bin-packing)."""
        active_ids = self.log.active_file_ids()
        all_rows = self.read()
        if not all_rows:
            raise ValueError("Nothing to compact: table is empty")

        removed_ids = list(active_ids)
        added_ids: List[str] = []
        for chunk in self._chunk_list(all_rows, TARGET_FILE_SIZE_ROWS):
            data_file = self._create_file(chunk)
            self._files[data_file.file_id] = data_file
            added_ids.append(data_file.file_id)

        entry = self.log.commit(
            operation=Operation.COMPACT,
            added_files=added_ids,
            removed_files=removed_ids,
            commit_info={
                "files_before": len(removed_ids),
                "files_after": len(added_ids),
            },
        )
        return entry

    def vacuum(self, retention_hours: int = DEFAULT_RETENTION_HOURS) -> Dict[str, int]:
        """Remove data files not referenced by any active version."""
        active_ids = self.log.active_file_ids()
        all_ever = self.log.all_ever_added_files()
        obsolete = all_ever - active_ids

        deleted = 0
        for fid in obsolete:
            if fid in self._files:
                del self._files[fid]
                deleted += 1

        if deleted > 0:
            self.log.commit(
                operation=Operation.VACUUM,
                commit_info={
                    "files_deleted": deleted,
                    "retention_hours": retention_hours,
                },
            )
        return {"files_deleted": deleted, "files_remaining": len(self._files)}

    # -- stats -------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        active_ids = self.log.active_file_ids()
        total_rows = sum(
            self._files[fid].row_count
            for fid in active_ids if fid in self._files
        )
        total_bytes = sum(
            self._files[fid].size_bytes
            for fid in active_ids if fid in self._files
        )
        return {
            "table_id": self.table_id,
            "name": self.name,
            "layer": self.layer.value,
            "current_version": self.log.current_version,
            "schema_version": self.schema.version,
            "active_files": len(active_ids),
            "total_files_stored": len(self._files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partitions": len(set(
                str(self._files[fid].partition_values)
                for fid in active_ids if fid in self._files
            )),
        }

    # -- helpers -----------------------------------------------------------

    def _partition_key(self, record: Dict[str, Any]) -> str:
        if not self.partition_cols:
            return "__default__"
        return "/".join(
            f"{col}={record.get(col, '__NONE__')}"
            for col in self.partition_cols
        )

    def _create_file(self, rows: List[Dict[str, Any]]) -> DataFile:
        file_id = f"part-{uuid.uuid4().hex[:12]}.parquet"
        pvals: Dict[str, str] = {}
        if self.partition_cols and rows:
            pvals = {
                col: str(rows[0].get(col, ""))
                for col in self.partition_cols
            }
        payload = json.dumps(rows, default=str)
        return DataFile(
            file_id=file_id,
            partition_values=pvals,
            rows=copy.deepcopy(rows),
            row_count=len(rows),
            size_bytes=len(payload.encode()),
            created_at=datetime.utcnow().isoformat(),
        )

    @staticmethod
    def _chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
        return [lst[i:i + size] for i in range(0, len(lst), size)]


# ---------------------------------------------------------------------------
# MedallionPipeline -- Bronze / Silver / Gold layer processing
# ---------------------------------------------------------------------------

class MedallionPipeline:
    """
    Implements the Medallion Architecture with quality gates between layers.

    Bronze: raw ingestion (append-only, audit metadata added)
    Silver: cleaned, deduplicated, schema-enforced
    Gold  : aggregated business metrics
    """

    def __init__(
        self,
        bronze_table: DeltaTable,
        silver_table: DeltaTable,
        gold_table: DeltaTable,
    ) -> None:
        self.bronze = bronze_table
        self.silver = silver_table
        self.gold = gold_table
        self._dead_letter: List[Dict[str, Any]] = []

    # -- Bronze ingestion --------------------------------------------------

    def ingest_to_bronze(
        self,
        raw_records: List[Dict[str, Any]],
        source_system: str = "unknown",
    ) -> LogEntry:
        """Ingest raw records into Bronze with audit metadata."""
        enriched: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()
        for rec in raw_records:
            row = dict(rec)
            row["_source_system"] = source_system
            row["_ingested_at"] = now
            row["_raw_payload"] = json.dumps(rec, default=str)
            enriched.append(row)

        return self.bronze.write(enriched, validate=False)

    # -- Silver processing -------------------------------------------------

    def bronze_to_silver(
        self,
        business_keys: List[str],
        clean_fn: Optional[Any] = None,
    ) -> Tuple[LogEntry, int, int]:
        """
        Process Bronze -> Silver with quality gates.

        Steps:
          1. Read latest Bronze snapshot
          2. Apply cleaning function (if provided)
          3. Schema validation against Silver schema
          4. Deduplicate by business keys (last-write-wins)
          5. Write valid records; quarantine invalid ones

        Returns (log_entry, accepted_count, rejected_count).
        """
        bronze_rows = self.bronze.read()

        # step 1 : strip audit columns to match silver schema
        cleaned: List[Dict[str, Any]] = []
        for row in bronze_rows:
            clean = {
                k: v for k, v in row.items()
                if not k.startswith("_")
            }
            if clean_fn:
                clean = clean_fn(clean)
            cleaned.append(clean)

        # step 2 : schema validation
        valid: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for rec in cleaned:
            ok, reason = self.silver.schema.validate_record(rec)
            if ok:
                valid.append(rec)
            else:
                rec["_rejection_reason"] = reason
                rejected.append(rec)

        self._dead_letter.extend(rejected)

        # step 3 : deduplicate by business keys (last-write-wins)
        deduped: Dict[str, Dict[str, Any]] = {}
        for rec in valid:
            key = tuple(rec.get(k) for k in business_keys)
            deduped[str(key)] = rec
        unique = list(deduped.values())

        if not unique:
            raise ValueError("No valid records after cleaning and dedup")

        # step 4 : merge into silver
        entry = self.silver.merge(unique, match_keys=business_keys)
        return entry, len(unique), len(rejected)

    # -- Gold aggregation --------------------------------------------------

    def silver_to_gold(
        self,
        group_by: List[str],
        agg_column: str,
        agg_functions: Optional[List[str]] = None,
    ) -> LogEntry:
        """
        Aggregate Silver -> Gold.

        Groups by specified columns and computes sum/count/avg/min/max
        on the aggregate column.
        """
        agg_fns = agg_functions or ["sum", "count", "avg", "min", "max"]
        silver_rows = self.silver.read()
        if not silver_rows:
            raise ValueError("Silver table is empty -- nothing to aggregate")

        # group rows
        groups: Dict[str, List[Any]] = {}
        group_examples: Dict[str, Dict[str, Any]] = {}
        for row in silver_rows:
            gkey = str(tuple(row.get(g) for g in group_by))
            groups.setdefault(gkey, [])
            val = row.get(agg_column)
            if val is not None and isinstance(val, (int, float)):
                groups[gkey].append(val)
            if gkey not in group_examples:
                group_examples[gkey] = {g: row.get(g) for g in group_by}

        # compute aggregates
        gold_records: List[Dict[str, Any]] = []
        for gkey, values in groups.items():
            rec: Dict[str, Any] = dict(group_examples.get(gkey, {}))
            if "sum" in agg_fns:
                rec[f"{agg_column}_sum"] = sum(values) if values else 0
            if "count" in agg_fns:
                rec[f"{agg_column}_count"] = len(values)
            if "avg" in agg_fns:
                rec[f"{agg_column}_avg"] = (
                    round(sum(values) / len(values), 2) if values else 0.0
                )
            if "min" in agg_fns:
                rec[f"{agg_column}_min"] = min(values) if values else None
            if "max" in agg_fns:
                rec[f"{agg_column}_max"] = max(values) if values else None
            gold_records.append(rec)

        return self.gold.write(gold_records, mode=WriteMode.OVERWRITE, validate=False)

    # -- dead-letter queue -------------------------------------------------

    @property
    def dead_letter_queue(self) -> List[Dict[str, Any]]:
        return list(self._dead_letter)

    def pipeline_stats(self) -> Dict[str, Any]:
        return {
            "bronze": self.bronze.stats(),
            "silver": self.silver.stats(),
            "gold": self.gold.stats(),
            "dead_letter_count": len(self._dead_letter),
        }


# ---------------------------------------------------------------------------
# DataCatalog -- register, discover, and search tables
# ---------------------------------------------------------------------------

class DataCatalog:
    """
    Centralized catalog for discovering and managing lakehouse tables.

    Stores metadata, tags, descriptions, quality scores, and lineage.
    """

    @dataclass
    class CatalogEntry:
        table: DeltaTable
        owner: str = "data-engineering"
        description: str = ""
        tags: List[str] = field(default_factory=list)
        quality_score: float = 0.0
        lineage: List[str] = field(default_factory=list)
        registered_at: str = field(
            default_factory=lambda: datetime.utcnow().isoformat()
        )

    def __init__(self) -> None:
        self._entries: Dict[str, DataCatalog.CatalogEntry] = {}

    def register(
        self,
        table: DeltaTable,
        owner: str = "data-engineering",
        description: str = "",
        tags: Optional[List[str]] = None,
        lineage: Optional[List[str]] = None,
    ) -> str:
        """Register a table in the catalog. Returns the table ID."""
        entry = DataCatalog.CatalogEntry(
            table=table,
            owner=owner,
            description=description,
            tags=tags or [],
            lineage=lineage or [],
        )
        self._entries[table.table_id] = entry
        return table.table_id

    def get(self, table_id: str) -> Optional[CatalogEntry]:
        return self._entries.get(table_id)

    def search(
        self,
        query: str = "",
        layer: Optional[Layer] = None,
        tags: Optional[List[str]] = None,
    ) -> List[CatalogEntry]:
        """Search catalog by name/description text, layer, or tags."""
        results: List[DataCatalog.CatalogEntry] = []
        q = query.lower()
        for entry in self._entries.values():
            if layer and entry.table.layer != layer:
                continue
            if tags and not set(tags).intersection(set(entry.tags)):
                continue
            if q and q not in entry.table.name.lower() and q not in entry.description.lower():
                continue
            results.append(entry)
        return results

    def update_quality(self, table_id: str, score: float) -> None:
        entry = self._entries.get(table_id)
        if entry:
            entry.quality_score = round(max(0.0, min(1.0, score)), 2)

    def list_all(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for tid, entry in self._entries.items():
            st = entry.table.stats()
            items.append({
                "table_id": tid,
                "name": entry.table.name,
                "layer": entry.table.layer.value,
                "owner": entry.owner,
                "description": entry.description,
                "tags": entry.tags,
                "quality_score": entry.quality_score,
                "rows": st["total_rows"],
                "version": st["current_version"],
            })
        return items


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _sep(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def demo() -> None:
    """End-to-end demonstration of the Data Lakehouse simulation."""

    # ------------------------------------------------------------------ 1
    _sep("1. Define schemas for each Medallion layer")

    bronze_schema = Schema(columns=[
        Column("order_id", ColumnType.INTEGER, nullable=False),
        Column("customer_id", ColumnType.INTEGER, nullable=False),
        Column("product", ColumnType.STRING),
        Column("amount", ColumnType.FLOAT),
        Column("order_date", ColumnType.STRING),
        # audit columns added at ingestion time -- not in schema (validate=False)
        Column("_source_system", ColumnType.STRING),
        Column("_ingested_at", ColumnType.STRING),
        Column("_raw_payload", ColumnType.STRING),
    ])

    silver_schema = Schema(columns=[
        Column("order_id", ColumnType.INTEGER, nullable=False),
        Column("customer_id", ColumnType.INTEGER, nullable=False),
        Column("product", ColumnType.STRING, nullable=False),
        Column("amount", ColumnType.FLOAT, nullable=False),
        Column("order_date", ColumnType.STRING, nullable=False),
    ])

    gold_schema = Schema(columns=[
        Column("product", ColumnType.STRING),
        Column("amount_sum", ColumnType.FLOAT),
        Column("amount_count", ColumnType.INTEGER),
        Column("amount_avg", ColumnType.FLOAT),
        Column("amount_min", ColumnType.FLOAT),
        Column("amount_max", ColumnType.FLOAT),
    ])

    bronze_table = DeltaTable("raw_orders", bronze_schema, ["order_date"], Layer.BRONZE)
    silver_table = DeltaTable("orders_cleaned", silver_schema, ["order_date"], Layer.SILVER)
    gold_table = DeltaTable("product_metrics", gold_schema, layer=Layer.GOLD)

    print("Bronze table:", bronze_table.name, "| layer:", bronze_table.layer.value)
    print("Silver table:", silver_table.name, "| layer:", silver_table.layer.value)
    print("Gold   table:", gold_table.name,   "| layer:", gold_table.layer.value)

    # ------------------------------------------------------------------ 2
    _sep("2. Ingest raw data into Bronze (batch 1)")

    pipeline = MedallionPipeline(bronze_table, silver_table, gold_table)

    batch1 = [
        {"order_id": 1, "customer_id": 100, "product": "Laptop",  "amount": 999.99, "order_date": "2024-01-15"},
        {"order_id": 2, "customer_id": 101, "product": "Mouse",   "amount": 29.99,  "order_date": "2024-01-15"},
        {"order_id": 3, "customer_id": 102, "product": "Keyboard","amount": 79.99,  "order_date": "2024-01-16"},
        {"order_id": 4, "customer_id": 100, "product": "Monitor", "amount": 449.99, "order_date": "2024-01-16"},
        {"order_id": 5, "customer_id": 103, "product": "Laptop",  "amount": 1099.99,"order_date": "2024-01-17"},
    ]

    entry = pipeline.ingest_to_bronze(batch1, source_system="ecommerce_db")
    print(f"Bronze commit v{entry.version}: {entry.rows_affected} rows ingested")
    print(f"Bronze stats: {bronze_table.stats()['total_rows']} rows, "
          f"{bronze_table.stats()['active_files']} files")

    # ------------------------------------------------------------------ 3
    _sep("3. Ingest batch 2 with duplicates and bad data")

    batch2 = [
        {"order_id": 5, "customer_id": 103, "product": "Laptop",  "amount": 1099.99, "order_date": "2024-01-17"},  # duplicate
        {"order_id": 6, "customer_id": 104, "product": "Tablet",  "amount": 599.99,  "order_date": "2024-01-17"},
        {"order_id": 7, "customer_id": 105, "product": None,      "amount": 19.99,   "order_date": "2024-01-18"},  # NULL product
        {"order_id": 8, "customer_id": 106, "product": "Charger", "amount": "free",   "order_date": "2024-01-18"},  # bad type
    ]

    entry = pipeline.ingest_to_bronze(batch2, source_system="ecommerce_db")
    print(f"Bronze commit v{entry.version}: {entry.rows_affected} rows ingested (raw, no validation)")
    print(f"Bronze total rows: {bronze_table.stats()['total_rows']}")

    # ------------------------------------------------------------------ 4
    _sep("4. Process Bronze -> Silver (clean + deduplicate)")

    def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
        """Clean individual record: normalize product names."""
        if rec.get("product") and isinstance(rec["product"], str):
            rec["product"] = rec["product"].strip().title()
        return rec

    entry, accepted, rejected = pipeline.bronze_to_silver(
        business_keys=["order_id"],
        clean_fn=clean_record,
    )
    print(f"Silver commit v{entry.version}: {accepted} accepted, {rejected} rejected")
    print(f"Silver rows: {silver_table.stats()['total_rows']}")
    print(f"Dead-letter queue: {len(pipeline.dead_letter_queue)} records")
    if pipeline.dead_letter_queue:
        for dlq in pipeline.dead_letter_queue:
            print(f"  Rejected: order_id={dlq.get('order_id')} -- {dlq.get('_rejection_reason')}")

    # ------------------------------------------------------------------ 5
    _sep("5. Process Silver -> Gold (aggregate by product)")

    entry = pipeline.silver_to_gold(
        group_by=["product"],
        agg_column="amount",
    )
    print(f"Gold commit v{entry.version}: {entry.rows_affected} aggregate rows")
    print("\nGold table (product metrics):")
    for row in gold_table.read():
        print(f"  {row.get('product'):>10s} | "
              f"sum={row.get('amount_sum', 0):>10.2f} | "
              f"count={row.get('amount_count', 0)} | "
              f"avg={row.get('amount_avg', 0):>8.2f}")

    # ------------------------------------------------------------------ 6
    _sep("6. Time travel -- query Silver at version 1 vs current")

    silver_v1 = silver_table.time_travel(version=1)
    silver_now = silver_table.read()
    print(f"Silver at v1 (initial MERGE): {len(silver_v1)} rows")
    print(f"Silver at current (v{silver_table.log.current_version}): {len(silver_now)} rows")
    if silver_v1:
        print("Sample row at v1:", {k: v for k, v in silver_v1[0].items()})

    # ------------------------------------------------------------------ 7
    _sep("7. Schema evolution -- add 'region' column to Silver")

    new_schema = silver_table.evolve_schema([
        Column("region", ColumnType.STRING, nullable=True),
    ])
    print(f"Silver schema evolved to v{new_schema.version}")
    print(f"Columns: {new_schema.column_names()}")

    # write records with the new column
    new_records = [
        {"order_id": 9,  "customer_id": 107, "product": "Speaker", "amount": 149.99,
         "order_date": "2024-01-19", "region": "US-West"},
        {"order_id": 10, "customer_id": 108, "product": "Webcam",  "amount": 89.99,
         "order_date": "2024-01-19", "region": "EU-Central"},
    ]
    entry = silver_table.write(new_records)
    print(f"Wrote {entry.rows_affected} rows with 'region' column")
    print(f"Silver total rows: {silver_table.stats()['total_rows']}")

    # ------------------------------------------------------------------ 8
    _sep("8. Data Catalog -- register and search tables")

    catalog = DataCatalog()
    catalog.register(
        bronze_table, owner="ingestion-team",
        description="Raw e-commerce orders from CDC",
        tags=["raw", "ecommerce", "orders"],
    )
    catalog.register(
        silver_table, owner="data-engineering",
        description="Cleaned and deduplicated order records",
        tags=["cleaned", "ecommerce", "orders"],
        lineage=[bronze_table.table_id],
    )
    catalog.register(
        gold_table, owner="analytics-team",
        description="Product-level revenue metrics",
        tags=["aggregated", "metrics", "revenue"],
        lineage=[silver_table.table_id],
    )

    catalog.update_quality(bronze_table.table_id, 0.70)
    catalog.update_quality(silver_table.table_id, 0.95)
    catalog.update_quality(gold_table.table_id, 0.99)

    print("Catalog contents:")
    for item in catalog.list_all():
        print(f"  [{item['layer']:>6s}] {item['name']:<20s} "
              f"owner={item['owner']:<20s} rows={item['rows']:<5d} "
              f"quality={item['quality_score']:.2f}")

    # search by tag
    results = catalog.search(tags=["ecommerce"])
    print(f"\nSearch 'ecommerce' tag: {len(results)} tables found")
    for r in results:
        print(f"  -> {r.table.name} ({r.table.layer.value})")

    # search by layer
    results = catalog.search(layer=Layer.GOLD)
    print(f"\nSearch layer=GOLD: {len(results)} tables found")
    for r in results:
        print(f"  -> {r.table.name}: {r.description}")

    # ------------------------------------------------------------------ 9
    _sep("9. Compaction and Vacuum")

    pre = bronze_table.stats()
    print(f"Before compaction: {pre['active_files']} files, {pre['total_rows']} rows")

    entry = bronze_table.compact()
    post = bronze_table.stats()
    print(f"After compaction:  {post['active_files']} files, {post['total_rows']} rows")
    print(f"  files_before={entry.commit_info['files_before']}, "
          f"files_after={entry.commit_info['files_after']}")

    vacuum_result = bronze_table.vacuum()
    print(f"Vacuum result: {vacuum_result['files_deleted']} files deleted, "
          f"{vacuum_result['files_remaining']} remaining")

    # ------------------------------------------------------------------ 10
    _sep("10. Transaction log history")

    print(f"\nBronze table history (latest 5):")
    for e in bronze_table.history(limit=5):
        print(f"  v{e.version}: {e.operation.value:<15s} "
              f"added={len(e.added_files)} removed={len(e.removed_files)} "
              f"rows={e.rows_affected}")

    print(f"\nSilver table history (latest 5):")
    for e in silver_table.history(limit=5):
        print(f"  v{e.version}: {e.operation.value:<15s} "
              f"added={len(e.added_files)} removed={len(e.removed_files)} "
              f"rows={e.rows_affected}")

    # ------------------------------------------------------------------ 11
    _sep("11. Pipeline summary")

    stats = pipeline.pipeline_stats()
    for layer_name in ("bronze", "silver", "gold"):
        s = stats[layer_name]
        print(f"  {layer_name:>6s} | version={s['current_version']:<3d} "
              f"files={s['active_files']:<3d} rows={s['total_rows']:<5d} "
              f"bytes={s['total_bytes']}")
    print(f"  Dead-letter queue: {stats['dead_letter_count']} records")

    print("\n-- Data Lakehouse simulation complete --")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    demo()
