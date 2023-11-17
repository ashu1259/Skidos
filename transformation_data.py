import os
import pandas as pd

def events_data(folder_path, output_file):
    # Getting a list of all CSV files in the folder
    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    # if there are not any CSV files in the folder
    if not csv_files:
        print("No CSV files found in the folder.")
        return
    # creating dataframe
    consolidated_data = pd.DataFrame()

    # Loop through each CSV file and append its data to the consolidated_data DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        df = pd.read_csv(file_path)
        consolidated_data = consolidated_data.append(df, ignore_index=True)

    # Write the consolidated data to a new CSV file
#     consolidated_data.to_csv(output_file, index=False)

    print("Consolidation complete.")
    return consolidated_data

folder_path = "C://Users//Abhishek//Downloads//EventsData"
output_file = 'C:/Users/Abhishek/Desktop/Output/consolidated_data.csv'
consolidated_data=events_data(folder_path, output_file)
def event_data_transformation(data):
    try:
        # filtering requered columns
        final_data=data[['event_name','os_version','player_id','platform','current_game']]
        # renaming columns
        rename_df_cols={'event_name':'Event Name','os_version':'SDK version','Player Id':'User ID',
                        'platform':'Platform','current_game':'Current Game'}
        final_data.rename(columns=rename_df_cols, inplace=True)
        # Pushing transformed data into output folder
        final_data.to_csv("C:/Users/Abhishek/Desktop/Output/transformed.csv")
        print("transformation competed")
    except Exception as e:
        print(f"An error occurred: {e}")
if consolidated_data is not None:
    event_data_transformation(consolidated_data)