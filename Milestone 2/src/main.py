from functions import *
from sqlalchemy import create_engine
import os
import time

table_names = ['Fintech_loans_clean','Lookup_table']

if not (check_clean_csv('Data/Fintech_loans_clean.csv') & check_clean_csv('Data/Lookup_table.csv')):
    print("Cleaned dataset not found. Processing raw dataset...\n")        
    df=pd.read_csv("Data/fintech_data_29_52_23411.csv")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    df=simple_cleaning(df)
    df=data_cleaning(df)
    df=handel_Outliers(df)
    df=feature_engineering(df)
    Encoded_df=Encode(df)
    Scaled_df=Scale(Encoded_df)
    final_df=final_cleaning(Scaled_df)
    Save_dfs(final_df, get_lookup())
    print(get_lookup())
else:
    print("Cleaned dataset found. Skipping data cleaning. \n")

#time.sleep(10)  # Delay for 10 seconds

con = establish_connection('fintech')   

for table_name in table_names:
    filename = f'Data/{table_name}.csv'
    upload_csv(filename = filename,table_name = table_name,engine=con)

