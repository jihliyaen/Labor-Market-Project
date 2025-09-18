# ------------------- Imports -------------------
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ------------------- Paths -------------------
DATA_DIR = r"C:\Users\acer\OneDrive\Labor Market Project\Data"

# ------------------- Phase 1: SOC → Categories → Jobs -------------------

# Mapping of O*NET Work Context variables -> 6 dimensions
CATEGORY_MAP = {
    # Communication
    "Face-to-Face_Discussions_with_Individuals_and_Within_Teams.xlsx": "Communication",
    "Public_Speaking.xlsx": "Communication",

    # Responsibility
    "Work_Outcomes_and_Results_of_Other_Workers.xlsx": "Responsibility",
    "Health_and_Safety_of_Other_Workers.xlsx": "Responsibility",
    "Determine_Tasks_Priorities_and_Goals.xlsx": "Responsibility",

    # Physical
    "Outdoors_Exposed_to_All_Weather_Conditions.xlsx": "Physical",
    "Physical_Proximity.xlsx": "Physical",

    # Criticality
    "Consequence of Error.xlsx": "Criticality",
    "Freedom of Decisions.xlsx": "Criticality",
    "Frequency of Decision Making.xlsx": "Criticality",
    
    # Routine
    "Degree_of_Automation.xlsx": "Routine",
    "Structured_vs_Unstructured_Work.xlsx": "Routine",

    # Skills
    "Job_Zone_One_Little_or_No_Preparation_Needed.xlsx": "Skills",
    "Job_Zone_Two_Some_Preparation_Needed.xlsx": "Skills",
    "Job_Zone_Three_Medium_Preparation_Needed.xlsx": "Skills",
    "Job_Zone_Four_Considerable_Preparation_Needed.xlsx": "Skills",
    "Job_Zone_Five_Extensive_Preparation_Needed.xlsx": "Skills",

    # Other data to be included but not categorized for Phase 1 output
    "AIOE_DataAppendix.xlsx": "AIOE" 
}

def load_and_merge_data(data_dir):
    """
    Loads multiple Excel files from a directory and merges them.
    Handles different file structures.
    """
    all_dfs = []
    logging.info("Starting to load and merge data from Excel files...")
    
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".xlsx") and file_name in CATEGORY_MAP:
            file_path = os.path.join(data_dir, file_name)
            try:
                df = pd.read_excel(file_path)
                df.columns = [col.strip() for col in df.columns]

                if "O*NET-SOC Code" in df.columns:
                    df = df.rename(columns={"O*NET-SOC Code": "SOC_Code"})
                elif "Code" in df.columns:
                    df = df.rename(columns={"Code": "SOC_Code"})
                
                if "Title" in df.columns:
                    df = df.rename(columns={"Title": "Occupation"})
                elif "Occupation" in df.columns:
                    df = df.rename(columns={"Occupation": "Occupation"})

                # Handle files with a single value column and no 'Element Name'
                value_col = None
                if "Data Value" in df.columns:
                    value_col = "Data Value"
                elif "Context" in df.columns:
                    value_col = "Context"
                elif "Job Zone" in df.columns:
                    value_col = "Job Zone"
                
                if value_col:
                    df = df.rename(columns={value_col: CATEGORY_MAP[file_name]})
                    df = df[["SOC_Code", "Occupation", CATEGORY_MAP[file_name]]]
                    all_dfs.append(df)
                    logging.info(f"Loaded {file_name}")
                else:
                    logging.warning(f"Skipping {file_name}: No recognized value column.")
            except Exception as e:
                logging.warning(f"Could not load {file_name}: {e}")

    if not all_dfs:
        logging.error("No relevant data files found to merge.")
        return pd.DataFrame()
    
    # Merge all DataFrames based on SOC_Code and Occupation
    merged_df = all_dfs[0]
    for df in all_dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["SOC_Code", "Occupation"], how="outer")
            
    logging.info(f"All data merged. Total shape: {merged_df.shape}")
    return merged_df

def collapse_to_soc(df):
    """
    Collapses variables into median category scores at the SOC level.
    """
    # Create a list of category columns to aggregate
    category_cols = list(set(CATEGORY_MAP.values()))
    
    # Aggregate using median, ensuring we don't include 'AIOE' in this step
    df_agg = df.groupby(["SOC_Code", "Occupation"]).median()
    
    # Clean up and select the columns you want for the output
    soc_df = df_agg[category_cols].reset_index()
    
    logging.info(f"Collapsed to SOC level: {soc_df.shape[0]} SOCs")
    return soc_df

if __name__ == "__main__":
    print("Phase 1: SOC → Categories → Jobs")

    # Step 1: Load and merge raw data from all relevant files
    raw_df = load_and_merge_data(DATA_DIR)
    
    if not raw_df.empty:
        # Step 2: Collapse to SOC-level dataset
        soc_df = collapse_to_soc(raw_df)

        # Save output
        output_file = os.path.join(DATA_DIR, "SOC_Categories_Phase1.xlsx")
        soc_df.to_excel(output_file, index=False)

        print(f" Phase 1 complete. Output saved to {output_file}")
        print(soc_df.head(10))