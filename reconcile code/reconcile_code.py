import os
import json
import pandas as pd

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, "r") as file:
        return json.load(file)

def get_correct_parent_folder(folder_path):
    """Returns the folder just before the last one in the given path."""
    parts = folder_path.split(os.sep)
    return parts[-2] if len(parts) > 2 else None

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
        if os.path.isdir(folder_path):
            extracted_folder = get_correct_parent_folder(folder_path)

            for filename in os.listdir(folder_path):
                if filename.startswith("~$"):  # Skip temporary files
                    continue

                if filename.endswith(('.xlsx', '.xls', '.csv', '.tsv')):
                    file_path = os.path.join(folder_path, filename)
                    
                    try:
                        df = pd.read_excel(file_path, engine='openpyxl')
                        df.columns = df.columns.str.strip().str.title()

                        available_columns = {col: new_col for col, new_col in column_mapping.items() if col in df.columns}
                        df = df[list(available_columns.keys())].rename(columns=available_columns)

                        # Date formatting
                        if "Date Post" in df.columns and "date_format" in system_config:
                            df["Date Post"] = pd.to_datetime(df["Date Post"], errors='coerce').dt.strftime(system_config["date_format"])
                        elif "Month" in df.columns and "date_format" in system_config:
                            df["Month"] = pd.to_datetime(df["Month"], errors='coerce').dt.strftime(system_config["date_format"])

                        # Add extra columns
                        for new_col, value in system_config.get("add_columns", {}).items():
                            if value == "folder_name":
                                df[new_col] = os.path.basename(folder_path)
                            elif value == "parent_folder_before_last":
                                df[new_col] = extracted_folder

                        all_data.append(df)
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        if "aggregate_functions" in system_config:
            summary_df = final_df.groupby(["Practitioner Name", "Date Post"], as_index=False).agg(system_config["aggregate_functions"])
        elif "Month" in final_df.columns:
            summary_df = final_df.groupby(["Practitioner Name", "Month"], as_index=False).sum()
        else:
            summary_df = final_df

        summary_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"Standardized summary saved at: {output_file_path}")
        return output_file_path  # Return path for reconciliation
    else:
        print("No data extracted.")
        return None

def process_excel_files(summary_df, file_c):
    """Performs reconciliation on the standardized data."""
    df_s = pd.read_excel(summary_df)
    df_c = pd.read_excel(file_c)
    
    # Determine which column to use for date
    if 'Date Post' in df_s.columns:
        date_column = 'Date Post'
    elif 'Month' in df_s.columns:
        date_column = 'Month'
    else:
        print("Error: Neither 'Date Post' nor 'Month' found in the standardized file.")
        print(f"Available columns in {summary_df}: {df_s.columns.tolist()}")
        return None  # Exit function if no valid date column
    
    df_s['Concat_Key'] = df_s['Practitioner Name'].astype(str) + '_' + df_s[date_column].astype(str)
    
    merged_df = df_s.merge(df_c, left_on='Concat_Key', right_on='Matchkey', how='inner')
    
    merged_df['EngageDiffCharges'] = (merged_df['MTDcharges'] - merged_df['Engage_Charges']).round(10)
    merged_df['EngageDiffPayments'] = (merged_df['MTDpayments'] - merged_df['Engage_Payments']).round(10)
    merged_df['EngageDiffAdjustments'] = (merged_df['EngageAdjustments'] - merged_df['Engage_Adjustments']).round(10)
    
    merged_df['Match_Status'] = merged_df.apply(
        lambda row: 'Match' if (row['EngageDiffCharges'] == 0 and 
                                row['EngageDiffPayments'] == 0 and 
                                row['EngageDiffAdjustments'] == 0) else 'Mismatch', axis=1)
    
    result_df = merged_df[['Concat_Key', 'MTDcharges', 'MTDpayments', 'Engage_Charges', 'Engage_Payments', 
                           'EngageAdjustments', 'Engage_Adjustments', 'EngageDiffCharges', 
                           'EngageDiffPayments', 'EngageDiffAdjustments', 'Match_Status']]
    
    return result_df  # Return reconciled DataFrame instead of saving immediately

def main():
    """Main function to standardize data and then perform reconciliation."""
    config_path = "test_config.json"
    config = load_config(config_path)
    
    all_reconciled_data = []  # List to store reconciled DataFrames

    for system_name in config["systems"]:
        standardized_file = process_system_data(system_name, config)
        
        if standardized_file:
            # Assuming 'c1.xlsx' is the file to reconcile against
            reconciled_df = process_excel_files(standardized_file, "c1.xlsx")
            if reconciled_df is not None:
                all_reconciled_data.append(reconciled_df)  # Store DataFrame in list

    # Combine all reconciled data into a single DataFrame and save to one Excel sheet
    if all_reconciled_data:
        final_combined_df = pd.concat(all_reconciled_data, ignore_index=True)
        combined_output_path = "final_combined_reconciliation.xlsx"
        final_combined_df.to_excel(combined_output_path, index=False, engine="openpyxl")

        print(f"All reconciled outputs saved in: {combined_output_path}")

if __name__ == "__main__":
    main()
