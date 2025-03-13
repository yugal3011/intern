import os
import json
import pandas as pd

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, "r") as file:
        return json.load(file)

def get_correct_parent_folder(folder_path):
    """
    Returns the folder just before the last one in the given path.
    Example: If folder_path is "C:/Users/Dell/Desktop/y/SourceSystem/intergy/Practitioner1",
    it should return "intergy".
    """
    parts = folder_path.split(os.sep)  # Split the path into folders
    if len(parts) > 2:  # Ensure there are enough parts in the path
        return parts[-2]  # Return the second last folder (before the last one)
    return None  # Return None if there's not enough depth

def process_system_data(system_name, config):
    """Processes data for a specific system based on the config."""
    system_config = config["systems"][system_name]
    column_mapping = system_config["columns"]
    output_filename = system_config["output_filename"]
    staging_folder = system_config["staging_folder"]
    root_directory = os.path.join(config["root_directory"], system_name)
    
    os.makedirs(staging_folder, exist_ok=True)  # Ensure output directory exists
    output_file_path = os.path.join(staging_folder, output_filename)
    
    all_data = []  # Store extracted data
    
    for foldername in os.listdir(root_directory):
        folder_path = os.path.join(root_directory, foldername)
        if os.path.isdir(folder_path):  # Check if it's a folder
            # Get the second-last folder (e.g., "intergy") and print it
            extracted_folder =get_correct_parent_folder(folder_path)
            print(extracted_folder)

            for filename in os.listdir(folder_path):
                if filename.startswith("~$"):  # Skip temporary Excel files
                    continue
                print(f"Processing file: {filename}")

                if filename.endswith(('.xlsx', '.xls', '.csv', '.tsv')):
                    file_path = os.path.join(folder_path, filename)
                    print(f"File path: {file_path}")
                    
                    try:
                        # Load Excel file
                        df = pd.read_excel(file_path, engine='openpyxl')

                        print(f"Detected columns in {filename}: {df.columns.tolist()}") # Prints detected column names

                        # Standardize column names
                        df.columns = df.columns.str.strip().str.title()#detects only columns present in 1st line of the file

                        # Identify available columns
                        available_columns = {col: new_col for col, new_col in column_mapping.items() if col in df.columns}
                        missing_columns = [col for col in column_mapping if col not in df.columns]
                        
                        if missing_columns:
                            print(f"Warning: Missing columns {missing_columns} in {file_path}")
                        
                        # Selecting only required columns
                        df = df[list(available_columns.keys())].rename(columns=available_columns)
                        

                        # Convert Date Post to 'Month_Year' format if applicable
                        if "Date Post" in df.columns and "date_format" in system_config:
                            df["Date Post"] = pd.to_datetime(df["Date Post"], errors='coerce').dt.strftime(system_config["date_format"])
                        elif "Month" in df.columns and "date_format" in system_config:
                            df["Month"] = pd.to_datetime(df["Month"], errors='coerce').dt.strftime(system_config["date_format"])
                        
                        # Add new columns as specified in the config
                        for new_col, value in system_config.get("add_columns", {}).items():
                            if value == "folder_name":
                                df[new_col] = os.path.basename(folder_path)  # Get last folder
                            elif value == "parent_folder_before_last":
                                df[new_col] = extracted_folder  # Get second-last folder (e.g., "intergy")

                        # Append to all_data list
                        all_data.append(df)
                        
                        print(f"Extracted and modified from: {file_path}")
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
    
    # Merge and summarize data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Group by Practitioner Name and Month/Date Post even if aggregation is missing
        if "aggregate_functions" in system_config:
            summary_df = final_df.groupby(["Practitioner Name", "Date Post"], as_index=False).agg(system_config["aggregate_functions"])
        elif "Month" in final_df.columns:
            summary_df = final_df.groupby(["Practitioner Name", "Month"], as_index=False).sum()
        else:
            summary_df = final_df
        
        # Save the summarized data
        summary_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"Summary file saved at: {output_file_path}")
    else:
        print("No data extracted. Check source files.")

def main():
    # Load the config file
    config_path = r"C:\Users\Dell\Desktop\y\final_config.json"
    config = load_config(config_path)
    
    # Process data for each system
    for system_name in config["systems"]:
        process_system_data(system_name, config)

if __name__ == "__main__":
    main()
#whatever changes are made to the code write here
#added a print statement in the line 54 for detecting columns present in the excel file
'''in line 94 , modified code to handle groupby functions even if aggregation function is missing in the config file
in future if you want to take unnecessary lines of code, 
remove lines 39 (if NECESSARY once again if neccesary also remove lines 44 & 48 because it prints filename and filepath)
code updated on github on tuesday 11-03-25 doesnt contain lines 94-100
changes need to be made is the date conversion on line 94 April_2025 to 2024-01 make this changes after some time'''