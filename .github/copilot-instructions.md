# Copilot Instructions

## Project Overview

This is a Python learning and practice repository containing implementations of data structures, algorithms, design patterns, and various utility scripts.

## Code Style Guidelines

- Use Python 3.9+ features and syntax
- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Write docstrings for functions and classes
- Keep functions focused and single-purpose

## Project Structure

- **DataStructures/** - Data structure implementations (LinkedList, BST, AVL Tree, Heap, HashMap, etc.)
- **Problems/** - Algorithm problems organized by category (Combinations, Sliding Window, etc.)
- **AI/** - AI/ML experiments using Google Generative AI
- **PySpark/** - Apache Spark DataFrame operations
- **Pandas/** - Pandas DataFrame operations
- **Utils/** - Utility scripts for various tasks
- **SystemDesign/** - System design implementations (Rate Limiter, Consistent Hashing, SSTable, etc.)
- **Design Patterns/** - Design pattern examples (Singleton, etc.)
- **tests/** - Test files using pytest

## Testing

- Use pytest for testing
- Test files should be prefixed with `test_`
- Place test files in the `tests/` directory or alongside the module being tested
- Run tests with `uv run pytest`

## Dependencies

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

- Base dependencies are minimal
- Optional dependency groups: `dev`, `ai`, `spark`, `pandas`, `utils`
- Install with `uv sync --extra <group>` or `uv sync --all-extras`

## When Writing Code

1. **Data Structures**: Implement clean, well-documented data structures with standard operations
2. **Algorithms**: Include time/space complexity comments, handle edge cases
3. **Problems**: Add problem description as comments at the top of the file
4. **Tests**: Write comprehensive test cases covering edge cases

## Preferences

- Prefer simple, readable solutions over clever one-liners
- Use descriptive variable and function names
- Add comments for complex logic
- Include example usage in docstrings when helpful
