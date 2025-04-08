# SparkTestify

**PySpark Data Pipeline Testing Framework**

SparkTestify is a lightweight, modular, and CI/CD-friendly testing framework built for PySpark data pipelines.

### Features
- Pytest Fixtures & Plugins for SparkSession
- DataFrame and Schema Assertions
- Mock Data Sources & Test Data Generation
- Integration Testing Support
- CI/CD ready (GitHub Actions, Pre-commit)

### Installation

```bash
pip install sparktestify
```

### Usage

```python
from sparktest.assertions import assert_dataframe_equal
from sparktest.fixtures import spark_session


def test_my_transformation(spark_session):
    input_df = spark_session.createDataFrame([...])
    output_df = my_transformation(input_df)
    expected_df = spark_session.createDataFrame([...])
    assert_dataframe_equal(output_df, expected_df)
```

### Development Setup

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
pre-commit install
pytest tests/
```

### CI/CD

GitHub Actions are configured to:
- Run pre-commit checks
- Run test cases on every push and PR

Workflow file: `.github/workflows/ci.yml`
