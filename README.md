# Nuthan

A collection of Python projects for learning and practicing data structures, algorithms, system design, and more.

**Requires Python >=3.10, <3.12** (constrained by tensorflow/spleeter compatibility).

## Contents

- **DataStructures/** — LinkedList, DoublyLinkedList, BST, AVL Tree, Red-Black Tree, B-Tree, B+ Tree, Heap, Trie, Fenwick Tree, Segment Tree, Skip List, Disjoint Set, LRU Cache, Bloom Filter, Graph, SortedSet, and more
- **Problems/** — Algorithm problems organized by category (Sliding Window, Combinations, BST, Trie, Union Find, Fenwick Tree)
- **SystemDesign/** — SSTable/LSM Tree, Consistent Hashing, Rendezvous Hashing, Rate Limiter, Load Balancer
- **LLD/** — Low-level design (Logger Module, Snake & Ladder, Airflow simulation)
- **AI/** — AI/ML experiments with Google Generative AI
- **PySpark/** — Apache Spark DataFrame operations
- **Pandas/** — Pandas DataFrame operations
- **Design Patterns/** — Singleton and other pattern examples
- **Utils/** — Logging utilities (LoggerFactory, sinks, priority buffer)
- **Adhoc/** — Miscellaneous scripts (JSON, HTTP requests, DeepDiff, etc.)
- **tests/** — Pytest test suite

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

### Install uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Install dependencies

```bash
# Install all dependencies
uv sync
```

> **Note:** All dependencies are currently in the main `[project.dependencies]` list in `pyproject.toml`.
> The `spleeter` package pulls in an older `pandas<2.0`; if you need a newer pandas version,
> consider removing `spleeter` from dependencies first.

### Run tests

```bash
uv run pytest
```

## Project Structure

```
Nuthan/
├── Adhoc/               # Miscellaneous scripts
├── AI/                  # AI/ML experiments (Google GenAI)
├── Basics/              # Beginner scripts and stdlib examples
├── DataStructures/      # Data structure implementations
├── DesignPatterns/      # Design pattern examples
├── LLD/                 # Low-level design (LoggerModule, games)
├── Misc/                # Docker, SQL notes, echo server
├── Pandas/              # Pandas DataFrame operations
├── Problems/            # Algorithm problems by category
│   ├── BST/
│   ├── Combinations/
│   ├── FenwickTree/
│   ├── SlidingWindow/
│   ├── Trie/
│   └── UnionFind/
├── PySpark/             # Spark DataFrame operations
│   └── sparkdata/       # Sample Spark/Delta data files
├── SystemDesign/        # System design implementations
├── Utils/               # Logging utilities
├── tests/               # Pytest test files
└── pyproject.toml       # Project configuration
```
