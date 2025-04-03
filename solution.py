import os
import json
import pandas as pd

def find_json_files(base_dir):
    """
    Recursively finds all JSON files in the given directory and its subdirectories.
    
    Args:
        base_dir (str): Base directory to search
        
    Returns:
        list: List of full paths to JSON files
    """
    json_files = []
    exclude_dirs = ['_MACOSX', '.DS_Store']
    
    # Check if base_dir exists
    if not os.path.exists(base_dir):
        print(f"Warning: Directory {base_dir} does not exist")
        return json_files
    
    for root, dirs, files in os.walk(base_dir):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    return sorted(json_files)

def merge_json_files(json_files):
    """
    Merges all JSON files from the provided list of file paths.
    
    Args:
        json_files (list): List of paths to JSON files
    
    Returns:
        pandas.DataFrame: Merged data from all JSON files
    """
    # List to store all records
    all_records = []
    
    for file_path in json_files:
        # Extract category name from filename (without .json extension)
        filename = os.path.basename(file_path)
        category = os.path.splitext(filename)[0]
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # Add category to each record
                for item in data:
                    item['category'] = category
                
                all_records.extend(data)
                print(f"Processed {file_path}: {len(data)} records")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_records)
    
    # Check if 'id' column exists and 'sku' doesn't, then rename 'id' to 'sku'
    if 'id' in df.columns and 'sku' not in df.columns:
        df = df.rename(columns={'id': 'sku'})
    
    # Handle duplicates - keep the one from the later file
    # Sort by category (ascending order of filenames) and keep the last occurrence
    df = df.sort_values('category').drop_duplicates(subset='sku', keep='last')
    
    # Make sure required columns exist
    required_columns = ['sku', 'name', 'brand', 'category']
    for col in required_columns:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in data. Creating empty column.")
            df[col] = ""
    
    # Reorder columns to match required schema
    return df[required_columns]

def join_csv_files(merged_df, input_csv_path):
    """
    Joins the merged DataFrame with the input CSV file.
    
    Args:
        merged_df (pandas.DataFrame): Merged data from JSON files
        input_csv_path (str): Path to the input CSV file
    
    Returns:
        pandas.DataFrame: Joined data with all rows from input CSV
    """
    # Read input CSV
    input_df = pd.read_csv(input_csv_path)
    
    # Check if 'product_sku' column exists in input_df
    join_column = 'product_sku'
    if join_column not in input_df.columns:
        # Try to find a column that might contain SKUs
        sku_candidates = [col for col in input_df.columns if 'sku' in col.lower()]
        if sku_candidates:
            join_column = sku_candidates[0]
            print(f"Using '{join_column}' for joining instead of 'product_sku'")
        else:
            print("Warning: 'product_sku' column not found in input.csv")
            print(f"Available columns: {list(input_df.columns)}")
    
    # Convert both columns to string to avoid type mismatch
    if join_column in input_df.columns:
        input_df[join_column] = input_df[join_column].astype(str)
    
    if 'sku' in merged_df.columns:
        merged_df['sku'] = merged_df['sku'].astype(str)
    
    # Join on product_sku = sku, keeping all rows from input CSV
    joined_df = pd.merge(
        input_df,
        merged_df,
        left_on=join_column,
        right_on='sku',
        how='left'  # Left join to keep all rows from input_df
    )
    
    return joined_df

def main():
    # Paths - these can be adjusted based on your project structure
    base_dir = '.'  # Start from current directory
    data_dir = 'data'  # But primarily look in the data directory
    input_csv_path = 'input.csv'
    merged_output_path = 'merged_output.csv'
    joined_output_path = 'joined_output.csv'
    
    # Find where input.csv is located
    if not os.path.exists(input_csv_path):
        print(f"Warning: {input_csv_path} not found in current directory")
        # Try to find it
        for root, _, files in os.walk(base_dir):
            if 'input.csv' in files:
                input_csv_path = os.path.join(root, 'input.csv')
                print(f"Found input.csv at: {input_csv_path}")
                break
    
    # Find all JSON files in the project
    all_json_files = find_json_files(data_dir)
    if not all_json_files:
        # If no JSON files in data dir, try to find them in the entire project
        all_json_files = find_json_files(base_dir)
    
    if all_json_files:
        print(f"Found {len(all_json_files)} JSON files:")
        for file in all_json_files:
            print(f" - {file}")
    else:
        print("No JSON files found in the project")
        return
    
    # Step 1: Merge JSON files
    merged_df = merge_json_files(all_json_files)
    merged_df.to_csv(merged_output_path, index=False)
    print(f"Merged data saved to {merged_output_path}")
    
    # Step 2: Join with input CSV
    joined_df = join_csv_files(merged_df, input_csv_path)
    joined_df.to_csv(joined_output_path, index=False)
    print(f"Joined data saved to {joined_output_path}")
    print(f"Input rows: {len(pd.read_csv(input_csv_path))}, Output rows: {len(joined_df)}")

if __name__ == "__main__":
    main()