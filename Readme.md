# Technical Challenge - Data Processing - Marble

## Overview
This project processes a collection of JSON files and merges them with an input CSV file to generate structured output data files.

## Features
- Recursively searches for all JSON files in the data directory
- Processes files in alphabetical order
- Extracts product information and categorizes based on filename
- Handles duplicate SKUs by keeping the one from the latest file
- Joins merged data with input CSV file
- Preserves all rows from the input CSV


## Installation
```bash
pip install pandas
```

## How to Run
```bash
python solution.py
```

## Output Files
The script generates two CSV files:
- `merged_output.csv`: Contains the merged data from all JSON files
- `joined_output.csv`: Contains the joined data from merged_output and input.csv


## Solution Approach
1. Find all JSON files in the data directory recursively
2. Process each file to extract product data
3. Add category based on filename
4. Handle duplicates, keeping latest entries
5. Join with input.csv on product_sku = sku