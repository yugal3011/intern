import os
import json
import pandas as pd

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, "r") as file:
        return json.load(file)

def process_files(config):
    """Extracts, processes, and standardizes data from multiple formats."""
    root_folder = config["root_folder"]
    column_mapping = config["columns"]
    output_folder = config["staging_folder"]
    output_filename = config["output_filename"]
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(output_folder, output_filename)
    
    all_data = []  # Store extracted data
    
    for foldername in os.listdir(root_folder):
        folder_path = os.path.join(root_folder, foldername)
        if os.path.isdir(folder_path):
            practitioner_name = os.path.basename(folder_path)
            for filename in os.listdir(folder_path):
                if filename.startswith("~$") or not filename.endswith((".xlsx", ".xls", ".csv", ".tsv")):
                    continue
                
                file_path = os.path.join(folder_path, filename)
                try:
                    df = pd.read_excel(file_path, engine='openpyxl')
                    df.columns = df.columns.str.strip().str.title()
                    available_columns = {col: new_col for col, new_col in column_mapping.items() if col in df.columns}
                    
                    if not available_columns:
                        print(f"Skipping {file_path}, no matching columns found.")
                        continue
                    
                    df = df[list(available_columns.keys())].rename(columns=available_columns)
                    df["Practitioner Name"] = practitioner_name
                    
                    if "Date Post" in df.columns:
                        df["Date Post"] = pd.to_datetime(df["Date Post"], errors='coerce').dt.strftime('%B_%Y')
                    
                    all_data.append(df)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        summary_df = final_df.groupby(["Date Post", "Practitioner Name"], as_index=False).agg({
            "MTDpayments": "sum",
            "MTDcharges": "sum",
            "EngageAdjustments": "sum"
        })
        summary_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"Summary saved at: {output_file_path}")
    else:
        print("No data extracted. Check source files.")

if __name__ == "__main__":
    config_path = "standardized_config.json"  # Updated JSON file
    config_data = load_config(config_path)
    
    for config in config_data["config_files"]:
        process_files(config)
