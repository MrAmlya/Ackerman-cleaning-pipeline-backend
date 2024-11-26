import data_extraction
import data_aggregation_cleaning
import final_data_cleaning 
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# lst = [ 'Slovakia', 'Czechoslovakia', 'Czech Republic', 'Serbia', 'Croatia', 'Bosnia and Herzegovina',
# 'Liechtenstein', 'Cyprus', 'Albania', 'Moldova', 'Lithuania', 'Yugoslavia', 'Latvia', 'Bulgaria',
# 'Free City of Danzig', 'Estonia', 'Malta', 'Greece', 'Austria', 'Italy', 'Belgium', 'Spain',
# 'United Kingdom', 'Great Britain', 'Ireland', 'Luxembourg', 'Switzerland', 'Turkey', 'China',
# 'Indochina', 'Mongolia', 'Japan', 'United States of America', 'United States', 'Canada',
# 'Mexico','Portugal', 'Monaco', 'Morocco', 'Egypt', 'Libya', 'Israel', 'Syria', 'Iraq', 'Lebanon',
#  'El Salvador', 'Argentina', 'South Africa', 'U.S. Virgin Islands', 'British Mandate for Palestine',
#  'Tunisia', 'Algeria',  'Yemen', 'Dutch East Indies', 'Dutch Guyana', 'Brazil',
#  'Rhodesia', 'Tangier', 'Cuba', 'Uruguay', 'Macedonia', 'Iran', 'Senegal', 'Philippines', 'India',
#  'Paraguay', 'Venezuela', 'Bolivia', 'Peru', 'Colombia', 'Congo', 'Australia', 'Dominican Republic',
#  'South West Africa', 'Ottoman Empire']

PATH_EXTRACTION = "extracted_data"

PATH_CLEANING = "temp_clean"

input_category = input("Do you want to extract Place (P), Year(Y), Cause of Death(C) or Last Name? (Type only P,Y,C or L)")
input_string = input("Enter items separated by commas: ")
item_list = input_string.split(',')
item_list = [item.strip() for item in item_list]
print(item_list)

if input_category == "P":
    data_extraction.country_extraction(item_list)
elif input_category == "Y":
    data_extraction.year_extraction(item_list)
elif input_category == "C":
    data_extraction.cause_of_death(item_list)
elif input_category == "L":
    data_extraction.last_name(item_list)

data_aggregation_cleaning.file_reader(PATH_EXTRACTION,item_list)
final_data_cleaning.file_reader_final_cleaning(PATH_CLEANING,item_list)



