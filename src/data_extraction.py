import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

PATH = "./database/combined_csv.csv"

df = pd.read_csv(PATH)

def country_extraction(lst):
    for i in lst:
        result = pd.DataFrame()
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        df3 = pd.DataFrame()
        combined_df = pd.DataFrame()
        df1 = df[df['PERMANENT_PLACE_OF_RESIDENCE'].str.contains(i, case=False, na=False)]
        df2 = df[df['PLACE_OF_DEATH'].str.contains(i, case=False, na=False)]
        df3 = df[df['PLACE_OF_BIRTH'].str.contains(i, case=False, na=False)]
        combined_df = pd.concat([df1, df2, df3], axis=0)
        result = pd.concat([combined_df,result],ignore_index=True)
        result.to_csv('./extracted_data/'+i+'.csv')
        print("Extracted for",i)

def year_extraction(lst):
    for i in lst:
        df1 = pd.DataFrame()
        df1 = df[df['DATE_OF_DEATH'].str.contains(i, case=False, na=False)]
        df1.to_csv('./extracted_data/'+i+'.csv')
        print("Extracted for",i)

def cause_of_death(lst):
    for i in lst:
        df1 = pd.DataFrame()
        df1 = df[df['CAUSE_OF_DEATH'].str.contains(i, case=False, na=False)]
        df1.to_csv('./extracted_data/'+i+'.csv')
        print("Extracted for",i)

def last_name(lst):
    for i in lst:
        df1 = pd.DataFrame()
        df1 = df[df['LAST_NAME'].str.contains(i, case=False, na=False)]
        df1.to_csv('./extracted_data/'+i+'.csv')
        print("Extracted for",i)


# lst = ["1942"]
# year_extraction(lst)

