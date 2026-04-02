"""Task Scheduler -- Low-Level Design

A priority-aware task scheduler with dependency resolution, retry logic,
recurring tasks, and simulated execution.

Key design patterns:
    - Strategy: task actions are injectable callables
    - Observer: scheduler notifies listeners of lifecycle events
    - Template Method: TaskExecutor encapsulates the run-retry loop

Classes:
    TaskStatus, Priority, Task, RecurringTask, TaskResult,
    TaskExecutor, Scheduler
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Protocol


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TaskStatus(Enum):
    """Lifecycle states for a task."""
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


class Priority(Enum):
    """Task priority -- lower numeric value means higher priority."""
    HIGH = 1
    MEDIUM = 2
    LOW = 3


# ---------------------------------------------------------------------------
# Observer protocol
# ---------------------------------------------------------------------------

class TaskObserver(Protocol):
    """Observer notified on task lifecycle transitions."""

    def on_task_event(self, task: Task, old_status: TaskStatus, new_status: TaskStatus) -> None: ...


class LoggingObserver:
    """Concrete observer that prints lifecycle transitions."""

    def on_task_event(self, task: Task, old_status: TaskStatus, new_status: TaskStatus) -> None:
        print(f"  [event] {task.name}: {old_status.value} -> {new_status.value}")


# ---------------------------------------------------------------------------
# TaskResult
# ---------------------------------------------------------------------------

@dataclass
class TaskResult:
    """Captures the outcome of a single task execution attempt."""
    task_id: str
    success: bool
    output: Any = None
    error: str | None = None
    duration: float = 0.0


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

class Task:
    """A unit of work managed by the scheduler.

    Args:
        task_id: Unique identifier.
        name: Human-readable name.
        action: Callable that performs the work and returns a result.
        priority: Execution priority.
        max_retries: How many times to retry on failure.
        dependencies: Tasks that must complete before this one runs.
        scheduled_time: Earliest simulated time at which the task may run.
    """

    def __init__(
        self,
        task_id: str,
        name: str,
        action: Callable[[], Any],
        priority: Priority = Priority.MEDIUM,
        max_retries: int = 0,
        dependencies: list[Task] | None = None,
        scheduled_time: float = 0.0,
    ) -> None:
        self.task_id = task_id
        self.name = name
        self.action = action
        self.priority = priority
        self.status = TaskStatus.PENDING
        self.max_retries = max_retries
        self.retry_count = 0
        self.dependencies: list[Task] = dependencies or []
        self.scheduled_time = scheduled_time

        self.created_at: float = time.time()
        self.started_at: float | None = None
        self.completed_at: float | None = None

    # -- behaviour ---------------------------------------------------------

    def execute(self) -> Any:
        """Run the task action and return its result."""
        return self.action()

    def should_retry(self) -> bool:
        """Return True if the task has remaining retry attempts."""
        return self.retry_count < self.max_retries

    def dependencies_met(self) -> bool:
        """Return True when every dependency has completed."""
        return all(d.status == TaskStatus.COMPLETED for d in self.dependencies)

    # -- dunder ------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Task(id={self.task_id!r}, name={self.name!r}, "
            f"priority={self.priority.name}, status={self.status.value})"
        )


# ---------------------------------------------------------------------------
# RecurringTask
# ---------------------------------------------------------------------------

class RecurringTask(Task):
    """A task that re-schedules itself at a fixed interval after each run.

    Args:
        interval_seconds: Seconds between successive executions.
        max_occurrences: Stop recurring after this many successful runs
            (``None`` means unlimited).
    """

    def __init__(
        self,
        task_id: str,
        name: str,
        action: Callable[[], Any],
        interval_seconds: float,
        max_occurrences: int | None = None,
        priority: Priority = Priority.MEDIUM,
        max_retries: int = 0,
        scheduled_time: float = 0.0,
    ) -> None:
        super().__init__(
            task_id=task_id,
            name=name,
            action=action,
            priority=priority,
            max_retries=max_retries,
            scheduled_time=scheduled_time,
        )
        self.interval_seconds = interval_seconds
        self.max_occurrences = max_occurrences
        self.occurrence_count = 0
        self.next_run_time = scheduled_time

    def advance_schedule(self) -> None:
        """Move *next_run_time* forward by one interval and reset state
        so the task can execute again."""
        self.occurrence_count += 1
        self.next_run_time += self.interval_seconds
        self.scheduled_time = self.next_run_time
        self.status = TaskStatus.PENDING
        self.retry_count = 0

    def has_more_occurrences(self) -> bool:
        """Return True if the task should keep recurring."""
        if self.max_occurrences is None:
            return True
        return self.occurrence_count < self.max_occurrences

    def __repr__(self) -> str:
        return (
            f"RecurringTask(id={self.task_id!r}, name={self.name!r}, "
            f"interval={self.interval_seconds}s, "
            f"occurrences={self.occurrence_count}, status={self.status.value})"
        )


# ---------------------------------------------------------------------------
# TaskExecutor
# ---------------------------------------------------------------------------

class TaskExecutor:
    """Runs a task, captures timing & exceptions, and returns a TaskResult."""

    def run(self, task: Task) -> TaskResult:
        """Execute *task* once and return the result.

        The task's ``started_at`` / ``completed_at`` timestamps are set here
        (using wall-clock time for bookkeeping).
        """
        task.started_at = time.time()
        start = time.monotonic()
        try:
            output = task.execute()
            duration = time.monotonic() - start
            task.completed_at = time.time()
            return TaskResult(
                task_id=task.task_id,
                success=True,
                output=output,
                duration=duration,
            )
        except Exception as exc:
            duration = time.monotonic() - start
            task.completed_at = time.time()
            return TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(exc),
                duration=duration,
            )


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class Scheduler:
    """Priority-aware scheduler with dependency resolution and retry logic.

    Usage::

        sched = Scheduler()
        sched.add_task(task_a)
        sched.add_task(task_b)
        sched.run(duration=30, time_step=1)

    Args:
        executor: TaskExecutor instance (created automatically if omitted).
    """

    def __init__(self, executor: TaskExecutor | None = None) -> None:
        self.tasks: dict[str, Task] = {}
        self.executor = executor or TaskExecutor()
        self.results: list[TaskResult] = []
        self._observers: list[TaskObserver] = []

    # -- observer management -----------------------------------------------

    def add_observer(self, observer: TaskObserver) -> None:
        self._observers.append(observer)

    def _notify(self, task: Task, old: TaskStatus, new: TaskStatus) -> None:
        for obs in self._observers:
            obs.on_task_event(task, old, new)

    def _set_status(self, task: Task, new_status: TaskStatus) -> None:
        old = task.status
        task.status = new_status
        self._notify(task, old, new_status)

    # -- task management ---------------------------------------------------

    def add_task(self, task: Task) -> None:
        """Register a task with the scheduler."""
        if task.task_id in self.tasks:
            raise ValueError(f"Duplicate task id: {task.task_id!r}")
        self.tasks[task.task_id] = task
        self._set_status(task, TaskStatus.SCHEDULED)

    def cancel_task(self, task_id: str) -> None:
        """Cancel a task if it hasn't started running yet."""
        task = self.tasks.get(task_id)
        if task is None:
            raise KeyError(f"Unknown task id: {task_id!r}")
        if task.status in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            self._set_status(task, TaskStatus.CANCELLED)
        else:
            raise RuntimeError(
                f"Cannot cancel task {task_id!r} in state {task.status.value}"
            )

    # -- scheduling logic --------------------------------------------------

    def get_ready_tasks(self, current_time: float) -> list[Task]:
        """Return tasks whose time has come and whose dependencies are met.

        Tasks are sorted by priority (HIGH first).
        """
        ready: list[Task] = []
        for task in self.tasks.values():
            if task.status not in (TaskStatus.SCHEDULED, TaskStatus.PENDING, TaskStatus.RETRYING):
                continue
            scheduled = (
                task.next_run_time if isinstance(task, RecurringTask) else task.scheduled_time
            )
            if current_time < scheduled:
                continue
            if not task.dependencies_met():
                continue
            ready.append(task)

        ready.sort(key=lambda t: t.priority.value)
        return ready

    def tick(self, current_time: float) -> None:
        """Advance one time step: execute all ready tasks in priority order."""
        for task in self.get_ready_tasks(current_time):
            self._run_task(task)

    def _run_task(self, task: Task) -> None:
        """Execute a single task and handle the result."""
        self._set_status(task, TaskStatus.RUNNING)
        result = self.executor.run(task)
        self.results.append(result)

        if result.success:
            self._set_status(task, TaskStatus.COMPLETED)
            # Reschedule recurring tasks
            if isinstance(task, RecurringTask) and task.has_more_occurrences():
                task.advance_schedule()
        else:
            if task.should_retry():
                task.retry_count += 1
                backoff = 2 ** (task.retry_count - 1)
                task.scheduled_time += backoff
                if isinstance(task, RecurringTask):
                    task.next_run_time = task.scheduled_time
                self._set_status(task, TaskStatus.RETRYING)
                print(f"  [retry] {task.name}: attempt {task.retry_count}/{task.max_retries} "
                      f"(backoff {backoff}s) -- {result.error}")
            else:
                self._set_status(task, TaskStatus.FAILED)
                print(f"  [failed] {task.name}: {result.error}")

    # -- simulation --------------------------------------------------------

    def run(self, duration: float, time_step: float = 1.0) -> None:
        """Simulate the scheduler for *duration* simulated seconds.

        Args:
            duration: Total simulated seconds to run.
            time_step: Granularity of each tick.
        """
        print(f"\n{'='*60}")
        print(f" Scheduler running for {duration}s (step={time_step}s)")
        print(f"{'='*60}")
        t = 0.0
        while t <= duration:
            ready = self.get_ready_tasks(t)
            if ready:
                print(f"\n[T]  t={t:.1f}s -- {len(ready)} task(s) ready")
            self.tick(t)
            t += time_step

    # -- reporting ---------------------------------------------------------

    def status(self) -> str:
        """Return a formatted summary of every task's current state."""
        lines = [f"\n{'='*60}", " Task Status Report", f"{'='*60}"]
        for task in self.tasks.values():
            extra = ""
            if isinstance(task, RecurringTask):
                extra = f" | runs={task.occurrence_count}"
            if task.retry_count:
                extra += f" | retries={task.retry_count}"
            lines.append(
                f"  [{task.priority.name:6s}] {task.name:<30s} "
                f"{task.status.value:<10s}{extra}"
            )
        lines.append(f"{'='*60}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Demo helpers -- simulated task actions
# ---------------------------------------------------------------------------

def make_action(name: str, duration: float = 0.0, fail_prob: float = 0.0) -> Callable[[], str]:
    """Return a callable that simulates work.

    Args:
        name: Label printed when the action runs.
        duration: Simulated sleep (kept tiny for demo).
        fail_prob: Probability in [0, 1] that the action raises.
    """
    def _action() -> str:
        if random.random() < fail_prob:
            raise RuntimeError(f"{name} encountered a transient error")
        time.sleep(duration)
        return f"{name} completed successfully"
    return _action


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

def main() -> None:
    random.seed(42)

    scheduler = Scheduler()
    scheduler.add_observer(LoggingObserver())

    # 1. Tasks with different priorities
    task_a = Task("a", "Data Ingestion", make_action("Ingestion", duration=0.01),
                  priority=Priority.HIGH, scheduled_time=0)
    task_b = Task("b", "Schema Validation", make_action("Validation", duration=0.01),
                  priority=Priority.MEDIUM, scheduled_time=0)

    # 2. Task C depends on A and B
    task_c = Task("c", "Transform & Load", make_action("Transform", duration=0.01),
                  priority=Priority.LOW, dependencies=[task_a, task_b], scheduled_time=0)

    # 3. Recurring task -- runs every 5 simulated seconds, up to 3 times
    recurring = RecurringTask(
        "r1", "Health Check Ping", make_action("Ping", duration=0.005),
        interval_seconds=5, max_occurrences=3,
        priority=Priority.HIGH, scheduled_time=2,
    )

    # 4. Task that fails and retries (high failure probability)
    flaky = Task("f1", "Flaky API Call", make_action("API-Call", duration=0.01, fail_prob=0.7),
                 priority=Priority.MEDIUM, max_retries=3, scheduled_time=1)

    # 5. A task we will cancel before it runs
    to_cancel = Task("x1", "Cancelled Report", make_action("Report"),
                     priority=Priority.LOW, scheduled_time=100)

    # Register all tasks
    for t in [task_a, task_b, task_c, recurring, flaky, to_cancel]:
        scheduler.add_task(t)

    # Cancel one task to demonstrate cancellation
    scheduler.cancel_task("x1")

    # 6. Run the simulation
    scheduler.run(duration=20, time_step=1)

    # 7. Final report
    print(scheduler.status())

    # Summary counts
    completed = sum(1 for t in scheduler.tasks.values() if t.status == TaskStatus.COMPLETED)
    failed = sum(1 for t in scheduler.tasks.values() if t.status == TaskStatus.FAILED)
    cancelled = sum(1 for t in scheduler.tasks.values() if t.status == TaskStatus.CANCELLED)
    print(f"\nSummary: {completed} completed, {failed} failed, {cancelled} cancelled")


if __name__ == "__main__":
    main()


