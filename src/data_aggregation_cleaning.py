import pandas as pd
import numpy as np
import os
import traceback
import warnings
warnings.filterwarnings("ignore")


def extract_country_name(file_name):
    parts = file_name.split('_')
    country_name = parts[0]
    country_name = country_name.lower()
    
    return country_name


def file_reader(path,lst):
    try:
        files_and_directories = os.listdir(path)
        for item in files_and_directories:
            # print(item)
            csv_read = path+"/"+item
            country_name = csv_read.split("/")[-1].replace(".csv", "")
            if country_name in lst:
                print("Starting for",country_name)
    #             country_name = extract_country_name(item)
                df = pd.read_csv(csv_read)
                df = remove_dup(df)
                df = add_dob(df,country_name)
                df = add_dod(df,country_name)
                df = split_country(df,country_name)
                df = rename_cols(df,country_name)
                df = drop_cols(df,country_name)
                df = age_extract(df,country_name)
                df.loc[:,'Age Group'] = df['Age'].apply(categorize_age)
                df = split_permanent_place(df,country_name)
                df = split_deportation(df,country_name)
                df['Last Name'] = df['Last Name'].fillna('unknown')
                name = "./temp_clean/"+country_name+".csv"
                print(df.shape)
                
                df.to_csv(name,index=False)
            else:
                pass
        print("All ran successfully")

    except Exception as e:
        print(traceback.format_exc())
        print(e,item)
        pass
#         print(f"The specified directory '{path}' does not exist.")

        
def remove_dup(df):
    print("shape before removing df")
    print(df.shape)
    df = df.drop_duplicates(subset=['FIRST_NAME', 'LAST_NAME','DATE_OF_BIRTH'])
    print("shape after removing duplicates")
    print(df.shape)
    print("#"*100)
    return df

def add_dob(df,name):
    for index, row in df.iterrows():
      try:
          if len(str(row['DATE_OF_BIRTH'])) == 4:
              df.at[index, 'year_birth'] = row['DATE_OF_BIRTH']
          if len(str(row['DATE_OF_BIRTH']))>4:
              day, month, year = row['DATE_OF_BIRTH'].split('/')
              # print(day,month,year)
              df.at[index,'day_birth'] = day
              df.at[index,'month_birth'] = month
              df.at[index,'year_birth'] = year
      except:
        pass
    
    print("Date of Birth seperated for ",name)
    print("#"*100)
    return df

def add_dod(df,name):
    for index, row in df.iterrows():
      try:
          if len(str(row['DATE_OF_DEATH'])) == 4:
              df.at[index, 'year_death'] = row['DATE_OF_DEATH']
              # print(index)
          if len(str(row['DATE_OF_DEATH']))>4:
              day, month, year = row['DATE_OF_DEATH'].split('/')
              # print(day,month,year)
              df.at[index,'day_death'] = day
              df.at[index,'month_death'] = month
              df.at[index,'year_death'] = year
              # print(index)
      except Exception as e:
        pass
    print("Date of Death seperated for ",name)
    print("#"*100)
    return df

def split_country(df,name):
    df['Country of Birth'] = df['PLACE_OF_BIRTH'].str.split(',').str[-1].str.strip()
    print("Country seperated for ",name)
    print("#"*100)
    return df

def rename_cols(df,name):

    df.rename(columns={'FIRST_NAME': 'First Name', 'LAST_NAME': 'Last Name','MAIDEN_NAME':'Maiden Name','GENDER_VALUE':'Gender','FATHER_FIRST_NAME':'Father First Name','FATHER_LAST_NAME':'Father Last Name','MOTHER_FIRST_NAME':'Mother First Name','MOTHER_MAIDEN_NAME':'Mother Maiden Name','AGE':'Age','PLACE_OF_BIRTH':'Place of Birth','CITIZENSHIP':'Citizenship','SPOUSE_FIRST_NAME':'Spouse First Name','SPOUSE_MAIDEN_NAME':'Spouse Maiden Name','MATERIAL_STATUS':'Marital Status','PROFESSION':'Profession','PERMANENT_PLACE_OF_RESIDENCE':'Permanent Place of Residence','RESIDENCE_ADDRESS':'Residence Address','PLACE_DURNING_THE_WAR':'Place During the War','ORIGIN_OF_DEPORTATION':'Origin of Deportation','DESTINATION_OF_DEPORTATION_1':'Destination of Deportation 1','DESTINATION_OF_DEPORTATION_2':'Destination of Deportation 2','PLACE_OF_DEATH':'Place of Death','CAUSE_OF_DEATH':'Cause of Death','STATUS_ACCORDING_TO_SOURCE':'Status According to Source','SOURCE':'Source','PROVENANCE':'Provenance','SUBMITTER_LAST_NAME':'Submitter Last Name','SUBMITTER_FIRST_NAME':'Submitter First Name','RELATIONSHIP_TO_VICTIM':'Relationship to Victim','year_birth':'Birth Year','day_birth':'Birth Day','month_birth':'Birth Month','year_death':'Death Year','day_death':'Death Day','month_death':'Death Month'}, inplace=True)
    print("cols renamed for ",name)
    print("#"*100)
    return df

def drop_cols(df,name):
    try:
        df.drop(['CLUSTER_ID','RECORD_ID','DATE_OF_BIRTH','DATE_OF_DEATH'], axis="columns", inplace=True)
        return df
    except Exception as e:
        print("Cols 404 ",name)
        return df

def age_extract(df,name):
    for row,index in df.iterrows():
        try:
            row['Age'] = int(row['Death Year']) - int(row['Birth Year'])
        except:
            pass
    print("Age Extracted for",name)
    print("#"*100)
    return df
    

def categorize_age(age):
    try:
        if age <= 10:
            return "0-10"
        elif age <= 20:
            return "11-20"
        elif age <= 30:
            return "21-30"
        elif age <= 40:
            return "31-40"
        elif age <= 50:
            return "41-50"
        elif age <= 60:
            return "51-60"
        elif age <= 70:
            return "61-70"
        elif age <= 80:
            return "71-80"
        elif age <= 90:
            return "81-90"
        elif age <=100:
            return "90-100"
        else:
            return ""
    except:
        pass

def split_permanent_place(df,name):
    df.loc[:,'Permanent Place - Country'] = df['Permanent Place of Residence'].astype(str).str.split(',').str[-1].str.strip()
    df['Permanent Place - Country'].unique()
    print("Country of Permanent Place seperated for ",name)
    print("#"*100)
    return df

def split_deportation(df,name):
    df.loc[:, 'Country of Deportation'] = df['Destination of Deportation 1'].astype(str).str.split(',').str[-1].str.strip()
    df['Country of Deportation'].unique()
    print("Country of Deportation seperated for ",name)
    print("#"*100)
    return df
