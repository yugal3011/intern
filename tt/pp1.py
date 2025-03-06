import os
import json
import pandas as pd

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, "r") as file:
        return json.load(file)

def extract_and_merge_columns(root_folder, config_path, output_folder):
    # Load config settings
    config = load_config(config_path)
    required_columns = config["columns"]
    output_filename = config["output_filename"]
    
    os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist
    output_file_path = os.path.join(output_folder, output_filename)
    
    all_data = []  # List to store extracted data
    
    for foldername in os.listdir(root_folder):
        folder_path = os.path.join(root_folder, foldername)
        if os.path.isdir(folder_path):  # Check if it's a folder
            last_folder_name = os.path.basename(folder_path)  # Extract only last folder name
            for filename in os.listdir(folder_path):
                if filename.startswith("~$"):  # Skip temporary Excel files
                    print(f"Skipping temporary file: {filename}")
                    continue

                if filename.endswith(('.xlsx', '.xls', '.csv', '.tsv')):
                    file_path = os.path.join(folder_path, filename)
                    
                    try:
                        # Load Excel file
                        df = pd.read_excel(file_path, engine='openpyxl')

                        # Check if required columns exist in the file
                        available_columns = [col for col in required_columns if col in df.columns]
                        if not available_columns:
                            print(f"Skipping {file_path}, no matching columns found.")
                            continue

                        # Selecting required columns
                        df = df[available_columns]
                        
                        # Adding Practitioner Name if not in columns
                        if "Practitioner Name" in required_columns:
                            df["Practitioner Name"] = last_folder_name
                        
                        # Append to all_data list
                        all_data.append(df)
                        
                        print(f"Extracted from: {file_path}")
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
    
    # Merge all extracted data and write to a single Excel file
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"Final merged file saved at: {output_file_path}")
    else:
        print("No data extracted. Check source files.")

def add_practitioner_name_column(root_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist
    
    for foldername in os.listdir(root_folder):
        folder_path = os.path.join(root_folder, foldername)
        if os.path.isdir(folder_path):  # Check if it's a folder
            last_folder_name = os.path.basename(folder_path)  # Extract only last folder name
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
                        df['Practitioner Name'] = last_folder_name
                        
                        # Save the updated file in new folder
                        df.to_excel(new_file_path, index=False, engine='openpyxl')
                        
                        print(f"Updated and saved: {new_file_path}")
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")

# Provide paths to root directory and config file
root_directory = r"C:\Users\Dell\Desktop\tt\SourceSystem\ecw"
config_file_path = r"C:\Users\Dell\Desktop\tt\config1.json"
output_directory = r"C:\Users\Dell\Desktop\tt\Staging\ecw"

# Run both functions
add_practitioner_name_column(root_directory, output_directory)
extract_and_merge_columns(root_directory, config_file_path, output_directory)
