import pandas as pd
import numpy as np
import os
import traceback
import warnings
warnings.filterwarnings("ignore")

def process_dataframe(df):
    try:
        # Attempt to drop the specified column, skip if it fails
        df.drop(['Unnamed: 0'], axis='columns', inplace=True)
    except Exception as e:
        print("Skipped dropping 'Unnamed: 0' column:", e)
        pass

    try:
        # Replace 'nan' strings with empty strings, skip if it fails
        df = df.replace('nan', '')
    except Exception as e:
        print("Skipped replacing 'nan' with empty string:", e)
        pass

    try:
        # Regex replacements for special characters, skip if errors occur
        df.replace({"<>": ""}, regex=True, inplace=True)
    except Exception as e:
        print("Skipped replacing '<>':", e)
        pass

    try:
        df.replace({", ()": ""}, regex=True, inplace=True)
    except Exception as e:
        print("Skipped replacing ', ()':", e)
        pass

    try:
        df.replace({"<br/>": "\""}, regex=True, inplace=True)
    except Exception as e:
        print("Skipped replacing '<br/>':", e)
        pass

    try:
        df.replace({"11-20":"'11-20"}, regex=False, inplace=True)
    except Exception as e:
        print("Skipped replacing '11-20':", e)
        pass

    try:
        df.replace(r"\(|\)", "", regex=True, inplace=True)
    except Exception as e:
        print("Skipped removing parentheses:", e)
        pass

    try:
        df.replace({",,,": ","}, regex=True, inplace=True)
    except Exception as e:
        print("Skipped replacing ',,,':", e)
        pass

    try:
        df.replace({",,": ","}, regex=True, inplace=True)
    except Exception as e:
        print("Skipped replacing ',,':", e)
        pass

    try:
        # Removing leading commas from specific columns
        df['Place of Birth-Cleaned'] = df['Place of Birth'].str.replace("^,", "", regex=True)
    except Exception as e:
        print("Skipped cleaning 'Place of Birth':", e)
        pass

    try:
        df['Permanent Place of Residence-Cleaned'] = df['Permanent Place of Residence'].str.replace("^,", "", regex=True)
    except Exception as e:
        print("Skipped cleaning 'Permanent Place of Residence':", e)
        pass

    try:
        df['Place During the War-Cleaned'] = df['Place During the War'].str.replace("^,", "", regex=True)
    except Exception as e:
        print("Skipped cleaning 'Place During the War':", e)
        pass

    try:
        df['Place of Death-Cleaned'] = df['Place of Death'].str.replace("^,", "", regex=True)
    except Exception as e:
        print("Skipped cleaning 'Place of Death':", e)
        pass

    return df


def extract_country_name(file_name):
    parts = file_name.split('_')
    country_name = parts[0]
    country_name = country_name.lower()
    
    return country_name

def file_reader_final_cleaning(path,lst):
    # print("here is lst----",lst)
    try:
        files_and_directories = os.listdir(path)
        # print(path)
        for item in files_and_directories:
            csv_read = path+"/"+item
#             country_name = csv_read.split("/")[-1].replace(".csv", "")
            file_name_with_extension = os.path.basename(csv_read)
            file_name_without_extension, _ = os.path.splitext(file_name_with_extension)
            # print(file_name_without_extension)
            if file_name_without_extension in lst:
                try:
                    print("Starting for",file_name_without_extension)
        #             country_name = extract_country_name(item)
                    df = pd.read_csv(csv_read)
                    df = process_dataframe(df)
                    name = "./final_cleaned_files/"+file_name_without_extension+"Cleaned_Final.csv"
        #             print(df.shape)
                    df.to_csv(name,index=False,encoding = "utf-8-sig")
                    print("Cleaning Completed for",file_name_without_extension)
                except Exception as e:
    #                 print(traceback.format_exc())
                    print("skipped for ==========",file_name_without_extension,e)
                    pass
            else:
                # print(csv_read)
                pass

            
        print("All ran successfully")

    except Exception as e:
        print(traceback.format_exc())
        print(e,item)
        pass





# file_reader_final_cleaning("temp_clean","X")