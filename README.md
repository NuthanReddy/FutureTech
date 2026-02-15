# Nuthan

A collection of Python projects including:

- **DataStructures/** - Implementations of common data structures (LinkedList, BST, AVL Tree, Heap, HashMap, etc.)
- **Problems/** - Algorithm problems and solutions (Dynamic Programming, Sliding Window, Combinations, etc.)
- **AI/** - AI/ML experiments with Google AI
- **PySpark/** - Apache Spark DataFrame operations
- **Pandas/** - Pandas DataFrame operations
- **Utils/** - Various utility scripts
- **SystemDesign/** - System design implementations (Rate Limiter, Consistent Hashing, etc.)
- **Design Patterns/** - Design pattern implementations

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install dependencies

```bash
# Install base dependencies
uv sync

# Install with dev dependencies (pytest)
uv sync --group dev

# Install specific optional dependencies
uv sync --extra pandas
uv sync --extra ai

# Install multiple extras (but not audio - has pandas conflict)
uv sync --extra pandas --extra utils

# Note: 'audio' extra (spleeter) requires pandas<2.0
# Don't combine with 'pandas' extra
uv sync --extra audio
```

### Run tests

```bash
uv run pytest
```

## Project Structure

```
Nuthan/
├── AI/                  # AI/ML experiments
├── DataStructures/      # Data structure implementations
├── Design Patterns/     # Design pattern examples
├── Examples/            # Example code snippets
├── LLD/                 # Low-level design
├── Pandas/              # Pandas operations
├── Problems/            # Algorithm problems
├── PySpark/             # Spark operations
├── Server/              # Server implementations
├── SQL/                 # SQL examples
├── SystemDesign/        # System design implementations
├── Utils/               # Utility scripts
├── tests/               # Test files
└── pyproject.toml       # Project configuration
```
