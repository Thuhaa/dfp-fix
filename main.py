import os
from datetime import date, datetime
from pathlib import Path
import pandas as pd

today = date.today()


def write_df(df, filename):
    df.to_csv(filename, index=False)


def to_millis(d_str):
    if d_str == "-" or d_str == None or d_str == 'None':
        return 0
    return datetime.strptime(d_str, '%m/%d/%Y, %H:%M:%S').timestamp()


# Necessary files in constants
PROJECT = 'AccessAllData'
INDICATOR_LIST = 'indicator_list.csv'  # download from => DataFuturePlatform/Projects/AccessAllData/indicator_list.csv
SOURCE_LIST = 'sources_list.csv'  # download from => DataFuturePlatform/Sources/sources_list.csv
OUTPUT = 'output.csv'  # download from => DataFuturePlatform/Projects/AccessAllData/output.csv
COUNTRY_LIST = 'country_territory_groups.json'  # download from => DataFuturePlatform/Utilities/country_territory_groups.json
META_INDICATOR_LIST = 'meta_indicator_list.csv'  # download from => DataFuturePlatform/Projects/AccessAllData/meta_indicator_list.csv
BACKUP_DIR = 'backup'
KEY_COL = "Alpha-3 code"
BASE_FOLDER_FILE_FORMAT = "csv"
BASE_FOLDER_ROOT = "base"
ERROR_LOG_PATH = "error_log.csv"  # download from => DataFuturePlatform/Utilities/error_log.csv

if not Path(BACKUP_DIR).exists():
    Path(BACKUP_DIR).mkdir()  # Make a local backup directory if it doesn't exist

if not Path(BASE_FOLDER_ROOT).exists():
    Path(BASE_FOLDER_ROOT).mkdir()  # Make a local base folder if it doesn't exist

indicator_df = pd.read_csv(INDICATOR_LIST)
source_df = pd.read_csv(SOURCE_LIST)
output_df = pd.read_csv(OUTPUT)
indicator_meta_df = pd.read_csv(META_INDICATOR_LIST)
write_df(indicator_meta_df, Path(BACKUP_DIR, META_INDICATOR_LIST))  # Backup the indicator metadata list
C_L = pd.read_json(COUNTRY_LIST)

indicator_meta_df.set_index('Indicator ID', inplace=True)
notebook_indicators = {}
default_indicators = {}
indicator_meta_info = {}
filename_source_id_map = {}
notebook_indicators_source_id_map = {}
error_count = {}
updated_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

for index, row in indicator_df.iterrows():
    source_file = source_df.loc[row["Source ID"]]
    print("SOURCE FILE", source_file)
    if source_file is None:
        print("Invalid source id", row["Source ID"], "Skip processing")
        continue

    indicator_meta = None
    if row["Indicator ID"] in indicator_meta_df.index:
        indicator_meta = indicator_meta_df.loc[row["Indicator ID"]]
    else:
        indicator_meta_df.loc[row["Indicator ID"]] = [None, None, None, None]

    last_indicator_time = 0
    last_source_time = 0
    if indicator_meta is not None:
        last_indicator_time = to_millis(indicator_meta.get("Updated On")) if indicator_meta.get("Updated On") else 0
        last_source_time = to_millis(indicator_meta.get("Transformed On")) if indicator_meta.get(
            "Transformed On") else 0

    if row.get("Frequency") == "Daily" or last_indicator_time == 0 or last_indicator_time < last_source_time:
        print("Publishing indicator", row["Indicator ID"], " from source", row["Source ID"])
        # if(row["Indicator ID"]=='prevalenceofneurologicaldisordersfemale_pdnfihme'):
        # print(row["Source ID"])
        n, file_extension = os.path.splitext(str(source_file["Source URL"]))
        filename = str(source_file.get("SaveAs")) if source_file.get("SaveAs") else (
                str(row["Source ID"]) + file_extension)
        publish_notebook = str(row.get("Publish Notebook")) if row.get("Publish Notebook") else None

        if publish_notebook is not None:
            if publish_notebook not in notebook_indicators:
                notebook_indicators[publish_notebook] = []
                notebook_indicators_source_id_map[publish_notebook] = {}
            notebook_indicators[publish_notebook].append(
                {
                    "filename": filename,
                    "indicator": row["Indicator ID"]
                }
            )
            notebook_indicators_source_id_map[publish_notebook][row["Source ID"]] = source_file["Source Name"]
        else:
            if filename not in default_indicators:
                filename_source_id_map[filename] = {}
                filename_source_id_map[filename][row["Source ID"]] = source_file["Source Name"]
                default_indicators[filename] = []
            default_indicators[filename].append(row["Indicator ID"])


# Todo: This commented out stub is entirely dependent on the mssparkutils library, which is not available in this environment.4
# Note: It's purpose is to run specified notebooks and update the indicator metadata list with the updated time.
# For the local environment this is not necessary.

# if notebook_indicators is not None:
#     for notebook in notebook_indicators:
#         notebook = str(notebook).strip()
#         try:
#             print("Running notebook", str(notebook))
#             mssparkutils.notebook.run(str(notebook), 300, {
#                 "indicators": json.dumps(notebook_indicators[notebook]),
#                 "project": project
#             })
#             print("Done", str(notebook), "run")
#             for i in notebook_indicators[notebook]:
#                 indi = i["indicator"]
#                 indicator_meta_df.loc[indi]["Updated On"] = updated_time
#         except Exception as e:
#             print("Fail to run notebook ", notebook, "for indicators", json.dumps(notebook_indicators[notebook]), e)
#             if PROJECT not in error_count:
#                 error_count[PROJECT] = []
#             error_count[PROJECT].append({
#                 "Source IDs": list(notebook_indicators_source_id_map[notebook].keys()),
#                 "Source Names": list(notebook_indicators_source_id_map[notebook].values()),
#                 "ETL Process Stage": PROJECT + " Publish",
#                 "Notebook": str(notebook),
#                 "Error": str(notebook) + " notebook execution failed."
#             })


# c_l is the country list
key_df = pd.DataFrame(columns=[KEY_COL])
output_df = output_df[output_df[KEY_COL].isin(C_L[KEY_COL])]
key_df[KEY_COL] = C_L[~C_L[KEY_COL].isin(output_df[KEY_COL])][KEY_COL]
output_df = pd.concat([output_df, key_df], ignore_index=True)
output_df.set_index(KEY_COL, inplace=True)
cols = set(output_df.columns)

for filename in default_indicators:
    print("Executing", filename)
    try:
        base_filename = filename.rsplit(".")[0] + BASE_FOLDER_FILE_FORMAT

        path = Path(BASE_FOLDER_ROOT, base_filename)
        indicator_data_df = pd.read_csv(path)
        print(path)
        input("Press Enter")
        indicator_data_df.set_index(KEY_COL, inplace=True)
        update_cols = set(output_df.columns.to_list()).intersection(set(indicator_data_df.columns.to_list()))
        add_columns = set(indicator_data_df.columns.to_list()) - set(output_df.columns.to_list())
        remove_columns = set(output_df.columns.to_list()) - set(indicator_data_df.columns.to_list())
        output_df.update(indicator_data_df[update_cols])
        output_df = output_df.join(indicator_data_df[add_columns])
        print("Successfully saved", filename, "data in", OUTPUT)
        for indicator in default_indicators[filename]:
            indicator_meta_df.loc[indicator]["Updated On"] = updated_time
    except Exception as e:
        print(filename, "Execution Error", e)
        if PROJECT not in error_count:
            error_count[PROJECT] = []
        error_count[PROJECT].append({
            "Source IDs": list(filename_source_id_map[filename].keys()),
            "Source Names": list(filename_source_id_map[filename].values()),
            "ETL Process Stage": PROJECT + " Publish",
            "Notebook": "publish",
            "Error": "publish notebook execution failed for " + filename
        })

output_df.reset_index(inplace=True)
# output_df.drop(['index', 'level_0'], axis=1, inplace=True)
write_df(output_df, Path(OUTPUT))
print(OUTPUT, "Successfully updated")

if (len(error_count) != 0):
    error_log_df = pd.read_csv(ERROR_LOG_PATH)
    error_count_df = pd.DataFrame(error_count[PROJECT])
    error_log_df = pd.concat([error_log_df, error_count_df], ignore_index=True)
    write_df(error_log_df, ERROR_LOG_PATH)
write_df(indicator_meta_df, META_INDICATOR_LIST)
print("Final output generation for countries is completed")

