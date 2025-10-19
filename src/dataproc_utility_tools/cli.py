#!/usr/bin/env python3
"""
CLI tool for validating CSV schemas using SchemaAnalyzer
"""

import argparse
import os
from pathlib import Path
import sys

from .schema_analyzer import SchemaAnalyzer


def main():
    """Main function for the dqu-validate CLI tool"""
    parser = argparse.ArgumentParser(
        description="Validate CSV schemas and analyze data quality"
    )
    parser.add_argument(
        "csv_files", 
        nargs="+", 
        help="CSV files to analyze"
    )
    parser.add_argument(
        "--output-dir", 
        default=".", 
        help="Output directory for JSON reports (default: current directory)"
    )
    parser.add_argument(
        "--chunksize", 
        type=int, 
        default=10000, 
        help="Chunk size for reading large files (default: 10000)"
    )
    parser.add_argument(
        "--sample-size", 
        type=int, 
        default=5, 
        help="Number of samples per column (default: 5)"
    )
    parser.add_argument(
        "--inconsistency-sample-size", 
        type=int, 
        default=3, 
        help="Number of inconsistency samples (default: 3)"
    )
    parser.add_argument(
        "--numeric-threshold", 
        type=float, 
        default=0.95, 
        help="Threshold for numeric classification (default: 0.95)"
    )
    parser.add_argument(
        "--mixed-threshold", 
        type=float, 
        default=0.7, 
        help="Threshold for mixed classification (default: 0.7)"
    )
    parser.add_argument(
        "--high-null-threshold", 
        type=float, 
        default=0.1, 
        help="Threshold for high null classification (default: 0.1)"
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Process each CSV file
    for csv_file in args.csv_files:
        if not Path(csv_file).exists():
            print(f"Error: File not found: {csv_file}", file=sys.stderr)
            continue

        # Create analyzer with specified parameters
        analyzer = SchemaAnalyzer(
            chunksize=args.chunksize,
            sample_size=args.sample_size,
            inconsistency_sample_size=args.inconsistency_sample_size,
            numeric_threshold=args.numeric_threshold,
            mixed_threshold=args.mixed_threshold,
            high_null_threshold=args.high_null_threshold
        )

        # Analyze the schema
        print(f"Analyzing: {csv_file}")
        schema = analyzer.analyze(csv_file)

        # Generate output filename based on input
        base_name = Path(csv_file).stem
        output_file = Path(args.output_dir) / f"schema_analysis_{base_name}.json"
        
        # Save the schema
        analyzer.save_schema(str(output_file))
        print(f"Schema analysis saved to: {output_file}")

        # Print summary
        analyzer.print_summary()


if __name__ == "__main__":
    main()