# ✨ SparkTestify

**PySpark Data Pipeline Testing Framework**  
🔍 Test your PySpark ETL pipelines like a pro.  
🧩 Plug & Play Pytest-based testing toolkit with CI/CD-ready setup.

[![CI](https://github.com/LiamNomad/sparktestify/actions/workflows/ci.yml/badge.svg)](https://github.com/LiamNomad/sparktestify/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparktestify.svg)](https://pypi.org/project/sparktestify/)
[![License](https://img.shields.io/github/license/LiamNomad/sparktestify)](../../LICENSE)

---

### 🚀 Features
✅ Pytest Fixtures & Plugins for SparkSession  
✅ DataFrame & Schema Assertions  
✅ Mock Data Source Generation  
✅ Integration Testing Ready  
✅ CI/CD + Pre-commit + Black + isort + flake8 configured

---

### 📥 Installation

```bash
pip install sparktestify
```

---

### 📄 Quickstart

```python
from sparktest.assertions import assert_dataframe_equal
from sparktest.fixtures import spark

def test_my_transformation(spark):
    input_df = spark.createDataFrame([...])
    output_df = my_transformation(input_df)
    expected_df = spark.createDataFrame([...])
    assert_dataframe_equal(output_df, expected_df)
```

---

### 🫶 Contributing

Contributions are always welcome!  
Please check our [CONTRIBUTING.md](../../CONTRIBUTING.md) guide to get started.

**License:** [MIT](../../LICENSE)
