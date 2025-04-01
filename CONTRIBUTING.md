# Contributing to SparkTestify

Thank you for considering contributing to SparkTestify! We welcome contributions from the community.

## How to Contribute

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Install dependencies and pre-commit hooks**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   pre-commit install
   ```
4. **Write your code and tests**
5. **Run pre-commit and tests locally**
   ```bash
   pre-commit run --all-files
   pytest tests/
   ```
6. **Open a Pull Request**

## Code Style

We use:
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting

These are enforced via pre-commit hooks.
