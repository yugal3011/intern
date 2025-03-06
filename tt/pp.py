import os
import json
import pandas as pd

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, "r") as file:
        return json.load(file)

def extract_and_summarize_data(root_folder, config_path, output_folder):
    # Load configuration settings
    config = load_config(config_path)
    column_mapping = config["columns"]
    output_filename = config["output_filename"]
    
    os.makedirs(output_folder, exist_ok=True)  # Ensure output directory exists
    output_file_path = os.path.join(output_folder, output_filename)
    
    all_data = []  # Store extracted data
    
    for foldername in os.listdir(root_folder):
        folder_path = os.path.join(root_folder, foldername)
        if os.path.isdir(folder_path):  # Check if it's a folder
            practitioner_name = os.path.basename(folder_path)  # Extract practitioner name
            for filename in os.listdir(folder_path):
                if filename.startswith("~$"):  # Skip temporary Excel files
                    continue

                if filename.endswith(('.xlsx', '.xls', '.csv', '.tsv')):
                    file_path = os.path.join(folder_path, filename)
                    
                    try:
                        # Load Excel file
                        df = pd.read_excel(file_path, engine='openpyxl')

                        # Standardize column names
                        df.columns = df.columns.str.strip().str.title()

                        # Identify available columns
                        available_columns = {col: new_col for col, new_col in column_mapping.items() if col in df.columns}
                        missing_columns = [col for col in column_mapping if col not in df.columns]
                        
                        if missing_columns:
                            print(f"Warning: Missing columns {missing_columns} in {file_path}")
                        
                        # Selecting only required columns
                        df = df[list(available_columns.keys())].rename(columns=available_columns)

                        # Convert Date Post to 'Month_Year' format
                        if "Date Post" in df.columns:
                            df["Date Post"] = pd.to_datetime(df["Date Post"], errors='coerce').dt.strftime('%B_%Y')
                        
                        # Add Practitioner Name
                        df["Practitioner Name"] = practitioner_name
                        
                        # Append to all_data list
                        all_data.append(df)
                        
                        print(f"Extracted and modified from: {file_path}")
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
    
    # Merge and summarize data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Summarize data by month and practitioner (one row per practitioner per month)
        summary_df = final_df.groupby(["Date Post", "Practitioner Name"], as_index=False).agg({
            "MTDpayments": "sum",
            "MTDcharges": "sum",
            "EngageAdjustments": "sum"
        })
        
        # Save only the summarized data
        summary_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"Summary file saved at: {output_file_path}")
    else:
        print("No data extracted. Check source files.")

def add_practitioner_name_column(root_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist
    
    for foldername in os.listdir(root_folder):
        folder_path = os.path.join(root_folder, foldername)
        if os.path.isdir(folder_path):  # Check if it's a folder
            practitioner_name = os.path.basename(folder_path)  # Extract only last folder name
            for filename in os.listdir(folder_path):
                if filename.endswith(('.xlsx', '.xls', '.csv', '.tsv')):
                    file_path = os.path.join(folder_path, filename)
                    new_folder_path = os.path.join(output_folder, foldername)
                    os.makedirs(new_folder_path, exist_ok=True)  # Create subfolder in output folder
                    new_file_path = os.path.join(new_folder_path, filename)
                    
                    try:
                        # Load Excel file
                        df = pd.read_excel(file_path, engine='openpyxl')
                        
                        # Ensure column is added before saving
                        df['Practitioner Name'] = practitioner_name
                        
                        # Save the updated file in new folder
                        df.to_excel(new_file_path, index=False, engine='openpyxl')
                        
                        print(f"Updated and saved: {new_file_path}")
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")

# Provide paths to root directory and config file
root_directory = r"C:\Users\Dell\Desktop\tt\SourceSystem\intergy"
config_file_path = r"C:\Users\Dell\Desktop\tt\config.json"
output_directory = r"C:\Users\Dell\Desktop\tt\Staging\intergy"

# Run both functions
add_practitioner_name_column(root_directory, output_directory)
extract_and_summarize_data(root_directory, config_file_path, output_directory)
