# Dataproc Utility Tools

[![PyPI version](https://badge.fury.io/py/dataproc-utility-tools.svg)](https://badge.fury.io/py/dataproc-utility-tools)
[![Python Version](https://img.shields.io/pypi/pyversions/dataproc-utility-tools.svg)](https://pypi.org/project/dataproc-utility-tools/)
[![License](https://img.shields.io/pypi/l/dataproc-utility-tools.svg)](https://github.com/rodrigo-sr/dataproc_utility_tools/blob/main/LICENSE)

A comprehensive Python library for data quality analysis, schema validation, and dataset cleaning. Perfect for data engineers, analysts, and scientists who need to ensure data integrity in their pipelines.

## Features

- **Schema Analysis**: Automatically detect data types and inconsistencies in CSV files
- **Data Quality Validation**: Identify numeric, categorical, boolean, and mixed-type inconsistencies
- **Dataset Cleaning**: Separate consistent and inconsistent records for targeted processing
- **Report Generation**: Detailed JSON reports with samples and inconsistency analysis
- **Large File Support**: Efficient chunked processing for big datasets
- **CLI Interface**: Command-line tool for easy integration into data pipelines
- **PySpark Integration**: Generate PySpark StructType schemas from analysis results

## Installation

```bash
pip install dataproc-utility-tools
```

## Quick Start

### Command-Line Usage

Analyze a CSV file and generate a quality report:

```bash
# Analyze a single CSV file
dqu-validate data/application_train.csv

# Analyze multiple files with custom parameters
dqu-validate data/*.csv --output-dir reports --chunksize 5000

# Generate detailed analysis with custom thresholds
dqu-validate data/dataset.csv \
  --numeric-threshold 0.9 \
  --mixed-threshold 0.7 \
  --high-null-threshold 0.1
```

### Programmatic Usage

```python
from dataproc_utility_tools import SchemaAnalyzer

# Analyze data quality
analyzer = SchemaAnalyzer(chunksize=10000)
schema = analyzer.analyze('data/dataset.csv')

# View analysis summary
analyzer.print_summary()

# Save detailed report
analyzer.save_schema('reports/schema_analysis.json')
```

Clean datasets based on analysis:

```python
from dataproc_utility_tools import CleanDatasetProcessor

# Process dataset using analysis results
processor = CleanDatasetProcessor()
consistent_path, inconsistent_path, n_consistent, n_inconsistent = processor.process_from_analysis(
    analysis_json='reports/schema_analysis.json',
    csv_path='data/dataset.csv',
    output_dir='cleaned'
)

print(f"Processed {n_consistent} consistent records and {n_inconsistent} inconsistent records")
```

## Advanced Usage

### Custom Schema Loading

Load analyzed schemas for pandas dtype enforcement:

```python
from dataproc_utility_tools import schema_from_analysis_json
import pandas as pd

# Load inferred schema for consistent data types
dtypes = schema_from_analysis_json('reports/schema_analysis.json')
df = pd.read_csv('data/dataset.csv', dtype=dtypes)
```

### PySpark Schema Generation

Generate PySpark StructType schemas from analysis results:

```python
from dataproc_utility_tools import schema_to_pyspark_struct
from pyspark.sql.types import *

# Generate PySpark schema string from analysis
pyspark_schema_str = schema_to_pyspark_struct('reports/schema_analysis.json')

# Create actual PySpark schema using eval()
schema = eval(pyspark_schema_str)

# Use with Spark DataFrame reading
df = spark.read.csv('data/dataset.csv', header=True, schema=schema)
```

### Integration with Data Pipelines

```python
from dataproc_utility_tools import SchemaAnalyzer, CleanDatasetProcessor

def quality_control_pipeline(input_csv, output_dir='processed'):
    """Complete data quality pipeline"""
    
    # Step 1: Analyze data quality
    analyzer = SchemaAnalyzer()
    schema = analyzer.analyze(input_csv)
    
    # Step 2: Generate quality report
    analyzer.save_schema(f'{output_dir}/quality_report.json')
    
    # Step 3: Separate consistent/inconsistent records
    processor = CleanDatasetProcessor()
    consistent_path, inconsistent_path, n_consistent, n_inconsistent = processor.process_from_analysis(
        analysis_json=f'{output_dir}/quality_report.json',
        csv_path=input_csv,
        output_dir=output_dir
    )
    
    return {
        'consistent_records': n_consistent,
        'inconsistent_records': n_inconsistent,
        'consistent_file': consistent_path,
        'inconsistent_file': inconsistent_path
    }

# Run the pipeline
results = quality_control_pipeline('data/raw_data.csv')
print(f"Processed {results['consistent_records']} clean records")
```

## API Reference

### SchemaAnalyzer

Main class for schema analysis and inconsistency detection.

```python
from dataproc_utility_tools import SchemaAnalyzer

# Initialize with custom parameters
analyzer = SchemaAnalyzer(
    chunksize=10000,              # Rows per chunk for large files
    sample_size=5,                # Samples per column
    numeric_threshold=0.95,       # Threshold for numeric classification
    mixed_threshold=0.7,         # Threshold for mixed-type classification
    high_null_threshold=0.1      # High null percentage threshold
)

# Analyze CSV file
schema = analyzer.analyze('data/dataset.csv')

# Access analysis results
summary = analyzer.analysis_summary
issues = analyzer.get_issues_by_type()
column_report = analyzer.get_column_report('column_name')
```

### CleanDatasetProcessor

Class for cleaning datasets based on schema analysis.

```python
from dataproc_utility_tools import CleanDatasetProcessor

processor = CleanDatasetProcessor()

# Process using analysis JSON
consistent_path, inconsistent_path, n_consistent, n_inconsistent = processor.process_from_analysis(
    analysis_json='reports/schema_analysis.json',
    csv_path='data/dataset.csv'
)

# Process DataFrame directly
df_consistent, df_inconsistent = processor.process_dataframe(df, schema)
```

### Utility Functions

Additional utility functions for schema manipulation.

```python
from dataproc_utility_tools import (
    schema_from_analysis_json, 
    schema_to_pyspark_struct
)

# Load pandas-compatible schema
dtypes = schema_from_analysis_json('reports/schema_analysis.json')
df = pd.read_csv('data/dataset.csv', dtype=dtypes)

# Generate PySpark schema string
pyspark_schema_str = schema_to_pyspark_struct('reports/schema_analysis.json')
# Use with: schema = eval(pyspark_schema_str)
```

## Report Format

Analysis reports are generated in JSON format with comprehensive details:

```json
{
  "schema": {
    "column_name": {
      "inferred_type": "numeric|categorical|boolean|mixed",
      "samples": ["sample1", "sample2"],
      "null_count": 5,
      "null_percentage": 0.01,
      "inconsistencies": [...],
      "numeric_ratio": 0.95
    }
  },
  "summary": {
    "total_columns": 10,
    "total_rows": 10000,
    "columns_with_issues": 2,
    "total_issues": 5,
    "type_distribution": {"numeric": 3, "categorical": 6, "boolean": 1}
  }
}
```

## Supported Data Issues

- **Non-numeric values in numeric columns**
- **Numeric values in categorical columns**  
- **High null percentage detection**
- **Mixed data types in single columns**
- **Boolean representation inconsistencies**
- **Date format irregularities**

## Requirements

- Python 3.10+
- pandas >= 1.3
- numpy >= 1.21

## Development

```bash
# Clone repository
git clone https://github.com/rodrigo-sr/dataproc_utility_tools.git
cd dataproc_utility_tools

# Install in development mode
pip install -e .

# Run tests
pytest

# Build package
python -m build
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

[rodrigo-sr](https://github.com/rodrigo-sr)