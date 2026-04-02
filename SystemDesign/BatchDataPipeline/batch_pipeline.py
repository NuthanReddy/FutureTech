"""
Batch Data Pipeline (ETL/ELT) Simulator

Demonstrates a DAG-based pipeline orchestrator with:
- Multi-source data extraction
- Transformation stages (clean, deduplicate, aggregate, join)
- Data quality checks (null, row count, schema validation)
- Date-based partitioning
- Retry with exponential backoff
- Idempotent reruns
- Lineage tracking
"""

from __future__ import annotations

import hashlib
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class StageType(Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    QUALITY_CHECK = "quality_check"


class RunStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class QualityCheckType(Enum):
    NULL_CHECK = "null_check"
    ROW_COUNT = "row_count"
    SCHEMA_VALIDATION = "schema_validation"
    UNIQUENESS = "uniqueness"


class Severity(Enum):
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class Record:
    """Represents a single data record (row)."""
    data: Dict[str, Any]
    source: str = ""
    timestamp: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class Dataset:
    """A collection of records with schema metadata."""
    name: str
    records: List[Record] = field(default_factory=list)
    schema: Dict[str, str] = field(default_factory=dict)
    partition_key: str = "date"
    partition_value: str = ""

    @property
    def row_count(self) -> int:
        return len(self.records)

    def get_column_values(self, column: str) -> List[Any]:
        return [r.data.get(column) for r in self.records]

    def get_null_count(self, column: str) -> int:
        return sum(1 for v in self.get_column_values(column) if v is None)

    def get_unique_count(self, column: str) -> int:
        non_null = [v for v in self.get_column_values(column) if v is not None]
        return len(set(non_null))

    def deduplicate(self, key_column: str) -> "Dataset":
        """Remove duplicate records based on a key column."""
        seen: Set[Any] = set()
        unique_records: List[Record] = []
        for record in self.records:
            key = record.data.get(key_column)
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
        return Dataset(
            name=self.name + "_deduped",
            records=unique_records,
            schema=self.schema,
            partition_key=self.partition_key,
            partition_value=self.partition_value,
        )

    def filter_nulls(self, required_columns: List[str]) -> "Dataset":
        """Remove records where any required column is null."""
        clean_records = [
            r for r in self.records
            if all(r.data.get(col) is not None for col in required_columns)
        ]
        return Dataset(
            name=self.name + "_cleaned",
            records=clean_records,
            schema=self.schema,
            partition_key=self.partition_key,
            partition_value=self.partition_value,
        )

    def __repr__(self) -> str:
        return (
            f"Dataset(name={self.name!r}, rows={self.row_count}, "
            f"partition={self.partition_value})"
        )


@dataclass
class DataSource:
    """Represents an external data source (DB, API, file)."""
    name: str
    source_type: str  # "database", "api", "file"
    connection_config: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, str] = field(default_factory=dict)

    def extract(self, partition_date: str, row_count: int = 100) -> Dataset:
        """Simulate extracting data from the source."""
        records: List[Record] = []
        for i in range(row_count):
            record_data: Dict[str, Any] = {}
            for col_name, col_type in self.schema.items():
                record_data[col_name] = self._generate_value(
                    col_name, col_type, i
                )
            # Inject some nulls randomly (~5% chance per field)
            if random.random() < 0.05:
                nullable_cols = [
                    c for c in self.schema if c != "id"
                ]
                if nullable_cols:
                    null_col = random.choice(nullable_cols)
                    record_data[null_col] = None

            records.append(
                Record(data=record_data, source=self.name)
            )

        # Inject a few duplicate IDs (~3%)
        if records and "id" in self.schema:
            num_dupes = max(1, len(records) // 30)
            for _ in range(num_dupes):
                original = random.choice(records)
                dupe = Record(
                    data=dict(original.data),
                    source=self.name,
                )
                records.append(dupe)

        return Dataset(
            name=f"raw_{self.name}",
            records=records,
            schema=self.schema,
            partition_key="date",
            partition_value=partition_date,
        )

    @staticmethod
    def _generate_value(col_name: str, col_type: str, index: int) -> Any:
        if col_type == "int":
            return index + 1
        elif col_type == "float":
            return round(random.uniform(10.0, 1000.0), 2)
        elif col_type == "string":
            return f"{col_name}_{index}"
        elif col_type == "date":
            base = datetime(2024, 1, 1)
            return (base + timedelta(days=index % 365)).strftime("%Y-%m-%d")
        return None


# ---------------------------------------------------------------------------
# Quality Checks
# ---------------------------------------------------------------------------

@dataclass
class QualityCheckResult:
    check_type: QualityCheckType
    column: Optional[str]
    passed: bool
    severity: Severity
    message: str
    actual_value: Any = None
    threshold: Any = None


class DataQualityChecker:
    """Runs configurable data quality checks on datasets."""

    def __init__(self) -> None:
        self.results: List[QualityCheckResult] = []

    def null_check(
        self,
        dataset: Dataset,
        column: str,
        max_null_pct: float = 0.0,
        severity: Severity = Severity.ERROR,
    ) -> QualityCheckResult:
        """Check that null percentage in a column is below threshold."""
        if dataset.row_count == 0:
            result = QualityCheckResult(
                check_type=QualityCheckType.NULL_CHECK,
                column=column,
                passed=True,
                severity=severity,
                message=f"Null check on '{column}': PASS (empty dataset)",
            )
            self.results.append(result)
            return result

        null_count = dataset.get_null_count(column)
        null_pct = (null_count / dataset.row_count) * 100.0
        passed = null_pct <= max_null_pct

        result = QualityCheckResult(
            check_type=QualityCheckType.NULL_CHECK,
            column=column,
            passed=passed,
            severity=severity,
            message=(
                f"Null check on '{column}': "
                f"{'PASS' if passed else 'FAIL'} "
                f"(null%={null_pct:.1f}%, threshold={max_null_pct}%)"
            ),
            actual_value=null_pct,
            threshold=max_null_pct,
        )
        self.results.append(result)
        return result

    def row_count_check(
        self,
        dataset: Dataset,
        min_rows: int = 1,
        max_rows: Optional[int] = None,
        severity: Severity = Severity.ERROR,
    ) -> QualityCheckResult:
        """Check that row count is within expected range."""
        count = dataset.row_count
        passed = count >= min_rows
        if max_rows is not None:
            passed = passed and count <= max_rows

        range_str = f">={min_rows}"
        if max_rows is not None:
            range_str = f"{min_rows}-{max_rows}"

        result = QualityCheckResult(
            check_type=QualityCheckType.ROW_COUNT,
            column=None,
            passed=passed,
            severity=severity,
            message=(
                f"Row count check: {'PASS' if passed else 'FAIL'} "
                f"(actual={count}, expected {range_str})"
            ),
            actual_value=count,
            threshold={"min": min_rows, "max": max_rows},
        )
        self.results.append(result)
        return result

    def schema_check(
        self,
        dataset: Dataset,
        expected_columns: List[str],
        severity: Severity = Severity.CRITICAL,
    ) -> QualityCheckResult:
        """Validate that all expected columns are present."""
        if dataset.row_count == 0:
            actual_cols: set = set(dataset.schema.keys())
        else:
            actual_cols = set(dataset.records[0].data.keys())

        missing = set(expected_columns) - actual_cols
        passed = len(missing) == 0

        result = QualityCheckResult(
            check_type=QualityCheckType.SCHEMA_VALIDATION,
            column=None,
            passed=passed,
            severity=severity,
            message=(
                f"Schema check: {'PASS' if passed else 'FAIL'} "
                f"(missing={sorted(missing) if missing else 'none'})"
            ),
            actual_value=sorted(actual_cols),
            threshold=sorted(expected_columns),
        )
        self.results.append(result)
        return result

    def uniqueness_check(
        self,
        dataset: Dataset,
        column: str,
        severity: Severity = Severity.ERROR,
    ) -> QualityCheckResult:
        """Check that a column has all unique values."""
        total = dataset.row_count
        unique = dataset.get_unique_count(column)
        duplicates = total - unique
        passed = duplicates == 0

        result = QualityCheckResult(
            check_type=QualityCheckType.UNIQUENESS,
            column=column,
            passed=passed,
            severity=severity,
            message=(
                f"Uniqueness check on '{column}': "
                f"{'PASS' if passed else 'FAIL'} "
                f"(duplicates={duplicates})"
            ),
            actual_value=unique,
            threshold=total,
        )
        self.results.append(result)
        return result

    def get_summary(self) -> Dict[str, int]:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        return {"total": total, "passed": passed, "failed": failed}

    def get_blocking_failures(self) -> List[QualityCheckResult]:
        return [
            r for r in self.results
            if not r.passed and r.severity in (Severity.ERROR, Severity.CRITICAL)
        ]


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------

class DataPartitioner:
    """Manages date-based partitioning of datasets."""

    def __init__(self) -> None:
        self.partitions: Dict[str, Dict[str, Dataset]] = defaultdict(dict)

    def write_partition(self, dataset: Dataset) -> str:
        """Write a dataset to a partition. Returns the partition path."""
        path = (
            f"data-lake/{dataset.name}/"
            f"partition_date={dataset.partition_value}/"
        )
        self.partitions[dataset.name][dataset.partition_value] = dataset
        return path

    def read_partition(
        self, dataset_name: str, partition_value: str
    ) -> Optional[Dataset]:
        """Read a specific partition."""
        return self.partitions.get(dataset_name, {}).get(partition_value)

    def list_partitions(self, dataset_name: str) -> List[str]:
        """List all partition values for a dataset."""
        return sorted(self.partitions.get(dataset_name, {}).keys())

    def get_idempotency_key(
        self, dataset_name: str, partition_value: str
    ) -> str:
        """Generate an idempotency key for a partition write."""
        raw = f"{dataset_name}:{partition_value}"
        return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Pipeline Stages
# ---------------------------------------------------------------------------

@dataclass
class StageResult:
    stage_name: str
    status: RunStatus
    input_rows: int = 0
    output_rows: int = 0
    duration_sec: float = 0.0
    error_message: str = ""
    output_dataset: Optional[Dataset] = None
    attempt: int = 1


@dataclass
class Stage:
    """A single stage in the pipeline DAG."""
    name: str
    stage_type: StageType
    process_fn: Callable[..., Dataset]
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 3
    retry_backoff_sec: float = 1.0
    quality_checks: List[Dict[str, Any]] = field(default_factory=list)
    description: str = ""


@dataclass
class LineageEntry:
    """Tracks data lineage for a stage execution."""
    stage_name: str
    input_datasets: List[str]
    output_dataset: str
    row_count_in: int
    row_count_out: int
    timestamp: datetime
    partition_date: str


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Pipeline:
    """A DAG-based data pipeline definition."""

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self.stages: Dict[str, Stage] = {}
        self.id = str(uuid.uuid4())[:8]

    def add_stage(self, stage: Stage) -> None:
        self.stages[stage.name] = stage

    def get_execution_order(self) -> List[str]:
        """Topological sort of stages based on dependencies."""
        in_degree: Dict[str, int] = {
            name: 0 for name in self.stages
        }
        adj: Dict[str, List[str]] = defaultdict(list)

        for name, stage in self.stages.items():
            for dep in stage.depends_on:
                adj[dep].append(name)
                in_degree[name] += 1

        queue = [n for n, d in in_degree.items() if d == 0]
        order: List[str] = []

        while queue:
            queue.sort()
            node = queue.pop(0)
            order.append(node)
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(self.stages):
            raise ValueError("Pipeline DAG has a cycle!")
        return order

    def validate(self) -> List[str]:
        """Validate pipeline definition. Returns list of errors."""
        errors: List[str] = []
        for name, stage in self.stages.items():
            for dep in stage.depends_on:
                if dep not in self.stages:
                    errors.append(
                        f"Stage '{name}' depends on unknown stage '{dep}'"
                    )
        try:
            self.get_execution_order()
        except ValueError as e:
            errors.append(str(e))
        return errors


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    partition_date: str
    status: RunStatus = RunStatus.PENDING
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    lineage: List[LineageEntry] = field(default_factory=list)


class PipelineOrchestrator:
    """Executes pipelines with dependency resolution, retry, and idempotency."""

    def __init__(self) -> None:
        self.runs: Dict[str, PipelineRun] = {}
        self.partitioner = DataPartitioner()
        self.quality_checker = DataQualityChecker()
        self.completed_partitions: Set[str] = set()
        self.stage_outputs: Dict[str, Dataset] = {}

    def run_pipeline(
        self,
        pipeline: Pipeline,
        partition_date: str,
        force: bool = False,
    ) -> PipelineRun:
        """Execute a pipeline for a given partition date."""
        idempotency_key = f"{pipeline.name}:{partition_date}"
        if idempotency_key in self.completed_partitions and not force:
            print(
                f"  [SKIP] Pipeline '{pipeline.name}' already completed "
                f"for partition {partition_date} (idempotent skip)"
            )
            return self.runs[idempotency_key]

        errors = pipeline.validate()
        if errors:
            raise ValueError(
                f"Pipeline validation failed: {'; '.join(errors)}"
            )

        run_id = str(uuid.uuid4())[:8]
        run = PipelineRun(
            run_id=run_id,
            pipeline_name=pipeline.name,
            partition_date=partition_date,
            status=RunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self.runs[idempotency_key] = run

        print(f"\n{'=' * 68}")
        print(f"  Pipeline: {pipeline.name} | Run: {run_id}")
        print(f"  Partition: {partition_date}")
        print(f"{'=' * 68}")

        execution_order = pipeline.get_execution_order()
        print(f"  Execution order: {' -> '.join(execution_order)}")

        self.stage_outputs.clear()
        all_passed = True

        for stage_name in execution_order:
            stage = pipeline.stages[stage_name]
            result = self._execute_stage(stage, partition_date)
            run.stage_results[stage_name] = result

            if result.status == RunStatus.COMPLETED and result.output_dataset:
                self.stage_outputs[stage_name] = result.output_dataset

                # Track lineage
                input_names = [
                    self.stage_outputs[d].name
                    for d in stage.depends_on
                    if d in self.stage_outputs and self.stage_outputs[d]
                ]
                run.lineage.append(LineageEntry(
                    stage_name=stage_name,
                    input_datasets=input_names,
                    output_dataset=result.output_dataset.name,
                    row_count_in=result.input_rows,
                    row_count_out=result.output_rows,
                    timestamp=datetime.now(),
                    partition_date=partition_date,
                ))

                # Run quality checks if configured
                if stage.quality_checks:
                    qc_passed = self._run_quality_checks(
                        result.output_dataset, stage.quality_checks
                    )
                    if not qc_passed:
                        all_passed = False
                        print(
                            f"    [!] Quality check FAILURE in stage "
                            f"'{stage_name}' -- pipeline halted"
                        )
                        break

                # Store partition
                path = self.partitioner.write_partition(result.output_dataset)
                print(f"    Stored at: {path}")

            elif result.status == RunStatus.FAILED:
                all_passed = False
                print(
                    f"    [!] Stage '{stage_name}' FAILED after retries: "
                    f"{result.error_message}"
                )
                break

        run.completed_at = datetime.now()
        if all_passed:
            run.status = RunStatus.COMPLETED
            self.completed_partitions.add(idempotency_key)
        else:
            run.status = RunStatus.FAILED

        duration = (run.completed_at - run.started_at).total_seconds()
        print(f"\n  Result: {run.status.value.upper()} ({duration:.2f}s)")
        print(f"{'=' * 68}")
        return run

    def _execute_stage(
        self, stage: Stage, partition_date: str
    ) -> StageResult:
        """Execute a single stage with retry logic."""
        print(f"\n  [{stage.stage_type.value.upper()}] {stage.name}")

        # Gather input datasets from dependencies
        input_datasets: List[Dataset] = []
        for dep in stage.depends_on:
            if dep in self.stage_outputs:
                input_datasets.append(self.stage_outputs[dep])

        for attempt in range(1, stage.retry_count + 1):
            try:
                start = time.time()
                output = stage.process_fn(
                    partition_date=partition_date,
                    inputs=input_datasets,
                )
                elapsed = time.time() - start

                input_rows = sum(d.row_count for d in input_datasets)
                result = StageResult(
                    stage_name=stage.name,
                    status=RunStatus.COMPLETED,
                    input_rows=input_rows,
                    output_rows=output.row_count,
                    duration_sec=round(elapsed, 3),
                    output_dataset=output,
                    attempt=attempt,
                )
                print(
                    f"    OK: {input_rows} rows in -> "
                    f"{output.row_count} rows out "
                    f"({elapsed:.3f}s, attempt {attempt})"
                )
                return result

            except Exception as exc:
                wait = stage.retry_backoff_sec * (2 ** (attempt - 1))
                if attempt < stage.retry_count:
                    print(
                        f"    Attempt {attempt} failed: {exc} "
                        f"-- retrying in {wait:.1f}s"
                    )
                    time.sleep(min(wait, 2.0))  # Cap sleep for demo
                else:
                    return StageResult(
                        stage_name=stage.name,
                        status=RunStatus.FAILED,
                        error_message=str(exc),
                        attempt=attempt,
                    )

        # Unreachable but satisfies type checker
        return StageResult(
            stage_name=stage.name,
            status=RunStatus.FAILED,
            error_message="Exhausted retries",
        )

    def _run_quality_checks(
        self, dataset: Dataset, checks: List[Dict[str, Any]]
    ) -> bool:
        """Run quality checks on a dataset. Returns True if all pass."""
        all_passed = True
        for check_cfg in checks:
            check_type = check_cfg["type"]

            if check_type == "null_check":
                result = self.quality_checker.null_check(
                    dataset,
                    column=check_cfg["column"],
                    max_null_pct=check_cfg.get("max_null_pct", 1.0),
                    severity=Severity(check_cfg.get("severity", "error")),
                )
            elif check_type == "row_count":
                result = self.quality_checker.row_count_check(
                    dataset,
                    min_rows=check_cfg.get("min_rows", 1),
                    max_rows=check_cfg.get("max_rows"),
                    severity=Severity(check_cfg.get("severity", "error")),
                )
            elif check_type == "schema":
                result = self.quality_checker.schema_check(
                    dataset,
                    expected_columns=check_cfg["columns"],
                    severity=Severity(check_cfg.get("severity", "critical")),
                )
            elif check_type == "uniqueness":
                result = self.quality_checker.uniqueness_check(
                    dataset,
                    column=check_cfg["column"],
                    severity=Severity(check_cfg.get("severity", "error")),
                )
            else:
                print(f"    Unknown check type: {check_type}")
                continue

            status_label = "PASS" if result.passed else "FAIL"
            print(f"    QC [{status_label}] {result.message}")
            if not result.passed and result.severity != Severity.WARNING:
                all_passed = False

        return all_passed

    def backfill(
        self,
        pipeline: Pipeline,
        start_date: str,
        end_date: str,
    ) -> List[PipelineRun]:
        """Run a pipeline for a range of dates (backfill)."""
        from datetime import date as dt_date

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        runs: List[PipelineRun] = []
        current = start

        print(f"\n  Backfill: {start_date} to {end_date}")
        while current <= end:
            partition = current.strftime("%Y-%m-%d")
            run = self.run_pipeline(pipeline, partition)
            runs.append(run)
            current += timedelta(days=1)
        return runs

    def print_lineage(self, run: PipelineRun) -> None:
        """Print the data lineage for a pipeline run."""
        print(f"\n  Data Lineage for run {run.run_id}:")
        print(f"  {'-' * 60}")
        for entry in run.lineage:
            inputs = ", ".join(entry.input_datasets) if entry.input_datasets else "(source)"
            print(
                f"    {entry.stage_name}: "
                f"[{inputs}] -> {entry.output_dataset} "
                f"({entry.row_count_in} -> {entry.row_count_out} rows)"
            )

    def print_quality_summary(self) -> None:
        """Print a summary of all quality checks."""
        summary = self.quality_checker.get_summary()
        print(f"\n  Quality Check Summary:")
        print(f"  {'-' * 40}")
        print(f"    Total checks : {summary['total']}")
        print(f"    Passed       : {summary['passed']}")
        print(f"    Failed       : {summary['failed']}")
        failures = self.quality_checker.get_blocking_failures()
        if failures:
            print(f"    Blocking failures:")
            for f in failures:
                print(f"      - {f.message}")


# ---------------------------------------------------------------------------
# Demo: Build and run a full ETL pipeline
# ---------------------------------------------------------------------------

def build_demo_pipeline() -> Tuple[Pipeline, List[DataSource]]:
    """Build a sample sales analytics ETL pipeline."""

    # --- Data sources ---
    orders_source = DataSource(
        name="orders",
        source_type="database",
        connection_config={"host": "db.example.com", "database": "sales"},
        schema={
            "id": "int",
            "customer_id": "int",
            "product_id": "int",
            "amount": "float",
            "order_date": "date",
            "status": "string",
        },
    )

    customers_source = DataSource(
        name="customers",
        source_type="database",
        connection_config={"host": "db.example.com", "database": "crm"},
        schema={
            "id": "int",
            "name": "string",
            "email": "string",
            "city": "string",
            "signup_date": "date",
        },
    )

    products_source = DataSource(
        name="products",
        source_type="api",
        connection_config={"url": "https://api.example.com/products"},
        schema={
            "id": "int",
            "name": "string",
            "category": "string",
            "price": "float",
        },
    )

    sources = [orders_source, customers_source, products_source]

    # --- Stage functions ---

    def extract_orders(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        return orders_source.extract(partition_date, row_count=200)

    def extract_customers(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        return customers_source.extract(partition_date, row_count=50)

    def extract_products(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        return products_source.extract(partition_date, row_count=30)

    def transform_orders(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        """Clean, deduplicate, and filter null IDs from orders."""
        raw = inputs[0] if inputs else Dataset(name="empty")
        cleaned = raw.filter_nulls(["id", "customer_id", "amount"])
        deduped = cleaned.deduplicate("id")
        deduped.name = "transformed_orders"
        deduped.partition_value = partition_date
        return deduped

    def transform_customers(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        raw = inputs[0] if inputs else Dataset(name="empty")
        cleaned = raw.filter_nulls(["id", "name"])
        deduped = cleaned.deduplicate("id")
        deduped.name = "transformed_customers"
        deduped.partition_value = partition_date
        return deduped

    def transform_products(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        raw = inputs[0] if inputs else Dataset(name="empty")
        deduped = raw.deduplicate("id")
        deduped.name = "transformed_products"
        deduped.partition_value = partition_date
        return deduped

    def join_and_aggregate(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        """Join orders with customers and products, then aggregate."""
        orders_ds = inputs[0] if len(inputs) > 0 else Dataset(name="empty")
        customers_ds = inputs[1] if len(inputs) > 1 else Dataset(name="empty")
        products_ds = inputs[2] if len(inputs) > 2 else Dataset(name="empty")

        # Build lookup maps
        customer_map: Dict[Any, Dict[str, Any]] = {}
        for r in customers_ds.records:
            customer_map[r.data.get("id")] = r.data

        product_map: Dict[Any, Dict[str, Any]] = {}
        for r in products_ds.records:
            product_map[r.data.get("id")] = r.data

        # Enrich orders with customer and product info
        enriched_records: List[Record] = []
        for r in orders_ds.records:
            enriched = dict(r.data)
            cust = customer_map.get(r.data.get("customer_id"), {})
            enriched["customer_name"] = cust.get("name", "unknown")
            enriched["customer_city"] = cust.get("city", "unknown")

            prod = product_map.get(r.data.get("product_id"), {})
            enriched["product_name"] = prod.get("name", "unknown")
            enriched["product_category"] = prod.get("category", "unknown")

            enriched_records.append(Record(data=enriched, source="joined"))

        # Aggregate: total amount by city
        city_totals: Dict[str, float] = defaultdict(float)
        for r in enriched_records:
            city = r.data.get("customer_city") or "unknown"
            amount = r.data.get("amount", 0) or 0
            city_totals[city] += amount

        agg_records = [
            Record(
                data={
                    "city": city,
                    "total_amount": round(total, 2),
                    "partition_date": partition_date,
                },
                source="aggregation",
            )
            for city, total in sorted(city_totals.items(), key=lambda x: x[0] or "")
        ]

        return Dataset(
            name="fact_sales_by_city",
            records=enriched_records,
            schema={
                "id": "int",
                "customer_name": "string",
                "product_name": "string",
                "amount": "float",
                "customer_city": "string",
                "product_category": "string",
            },
            partition_key="date",
            partition_value=partition_date,
        )

    def load_warehouse(
        partition_date: str, inputs: List[Dataset]
    ) -> Dataset:
        """Simulate loading data into the warehouse (upsert/merge)."""
        ds = inputs[0] if inputs else Dataset(name="empty")
        ds.name = "warehouse_fact_sales"
        ds.partition_value = partition_date
        return ds

    # --- Build pipeline DAG ---
    pipeline = Pipeline(
        name="daily_sales_analytics",
        description="Daily ETL pipeline: orders + customers + products -> sales facts",
    )

    pipeline.add_stage(Stage(
        name="extract_orders",
        stage_type=StageType.EXTRACT,
        process_fn=extract_orders,
        description="Extract raw orders from sales database",
        quality_checks=[
            {"type": "row_count", "min_rows": 10, "severity": "error"},
            {"type": "schema", "columns": ["id", "customer_id", "amount"],
             "severity": "critical"},
        ],
    ))

    pipeline.add_stage(Stage(
        name="extract_customers",
        stage_type=StageType.EXTRACT,
        process_fn=extract_customers,
        description="Extract raw customers from CRM database",
    ))

    pipeline.add_stage(Stage(
        name="extract_products",
        stage_type=StageType.EXTRACT,
        process_fn=extract_products,
        description="Extract products from catalog API",
    ))

    pipeline.add_stage(Stage(
        name="transform_orders",
        stage_type=StageType.TRANSFORM,
        process_fn=transform_orders,
        depends_on=["extract_orders"],
        description="Clean, deduplicate, and validate orders",
        quality_checks=[
            {"type": "null_check", "column": "id", "max_null_pct": 0,
             "severity": "critical"},
            {"type": "uniqueness", "column": "id", "severity": "error"},
        ],
    ))

    pipeline.add_stage(Stage(
        name="transform_customers",
        stage_type=StageType.TRANSFORM,
        process_fn=transform_customers,
        depends_on=["extract_customers"],
        description="Clean and deduplicate customers",
    ))

    pipeline.add_stage(Stage(
        name="transform_products",
        stage_type=StageType.TRANSFORM,
        process_fn=transform_products,
        depends_on=["extract_products"],
        description="Deduplicate products",
    ))

    pipeline.add_stage(Stage(
        name="join_and_aggregate",
        stage_type=StageType.TRANSFORM,
        process_fn=join_and_aggregate,
        depends_on=[
            "transform_orders",
            "transform_customers",
            "transform_products",
        ],
        description="Join orders with dimensions and aggregate by city",
        quality_checks=[
            {"type": "row_count", "min_rows": 1, "severity": "error"},
            {"type": "null_check", "column": "customer_name",
             "max_null_pct": 10.0, "severity": "warning"},
        ],
    ))

    pipeline.add_stage(Stage(
        name="load_warehouse",
        stage_type=StageType.LOAD,
        process_fn=load_warehouse,
        depends_on=["join_and_aggregate"],
        description="Load fact table into data warehouse",
        quality_checks=[
            {"type": "row_count", "min_rows": 1, "severity": "error"},
        ],
    ))

    return pipeline, sources


def main() -> None:
    print("=" * 68)
    print("  BATCH DATA PIPELINE (ETL) SIMULATOR")
    print("=" * 68)

    # Build the pipeline
    pipeline, sources = build_demo_pipeline()

    # Validate the DAG
    print("\n-- Pipeline Definition --")
    print(f"  Name: {pipeline.name}")
    print(f"  Stages: {len(pipeline.stages)}")
    print(f"  Execution order: {' -> '.join(pipeline.get_execution_order())}")
    errors = pipeline.validate()
    if errors:
        print(f"  Validation ERRORS: {errors}")
        return
    print("  Validation: PASSED")

    print(f"\n-- Data Sources --")
    for src in sources:
        print(f"  {src.name} ({src.source_type}): {list(src.schema.keys())}")

    # Run the pipeline for a single partition
    orchestrator = PipelineOrchestrator()
    run1 = orchestrator.run_pipeline(pipeline, "2024-01-15")

    # Print lineage
    orchestrator.print_lineage(run1)

    # Demonstrate idempotent rerun (should skip)
    print("\n-- Idempotent Rerun Test --")
    orchestrator.run_pipeline(pipeline, "2024-01-15")

    # Force rerun
    print("\n-- Forced Rerun --")
    run2 = orchestrator.run_pipeline(pipeline, "2024-01-15", force=True)

    # Print partition info
    print(f"\n-- Partitions Written --")
    for ds_name in sorted(orchestrator.partitioner.partitions.keys()):
        parts = orchestrator.partitioner.list_partitions(ds_name)
        for p in parts:
            ds = orchestrator.partitioner.read_partition(ds_name, p)
            row_info = f"{ds.row_count} rows" if ds else "empty"
            print(f"  {ds_name}/partition_date={p}/ ({row_info})")

    # Quality summary
    orchestrator.print_quality_summary()

    # Backfill demo (2 days)
    print("\n-- Backfill Demo (2 days) --")
    backfill_runs = orchestrator.backfill(pipeline, "2024-01-13", "2024-01-14")
    succeeded = sum(1 for r in backfill_runs if r.status == RunStatus.COMPLETED)
    print(f"\n  Backfill complete: {succeeded}/{len(backfill_runs)} succeeded")

    # Final quality summary
    orchestrator.print_quality_summary()

    print("\n" + "=" * 68)
    print("  PIPELINE SIMULATION COMPLETE")
    print("=" * 68)


if __name__ == "__main__":
    main()
