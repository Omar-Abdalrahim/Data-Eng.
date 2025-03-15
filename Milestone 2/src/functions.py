import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, LabelEncoder,StandardScaler
from sqlalchemy import create_engine
import os

lookup_table = pd.DataFrame()

def establish_connection(db_name):
	print(f"Connecting to database: {db_name}")
	engine = create_engine(f'postgresql://root:root@postgres:5432/{db_name}')
	return engine

def check_clean_csv(file_to_check):
	if os.path.exists(file_to_check):
		return True
	return False

def upload_csv(filename, table_name, engine):  
    df = pd.read_csv(filename)
    try:
        df.to_sql(table_name, con=engine, if_exists='fail', index=False)
        print(f'{table_name}.csv file uploaded to the database as a table')
    except ValueError as e:
        print("Table already exists. Error:", e)
    return

def log_to_lookup_table(tmp,df,key,note):
    global lookup_table

    if key == "I":
        # Find differences
        changes_mask = (tmp != df)
        # Create a DataFrame to log changes
        changes = []
        # Iterate through the DataFrame to log changes
        for col in changes_mask.columns:
            for index in changes_mask.index:
                if changes_mask.at[index, col]:  # If there's a change
                    c=df.at[index, col]
                    if df[col].dtype != 'object':
                        c=str(c)+" ("+note+")"
                    changes.append({
                    'Column': col,
                    'Original': tmp.at[index, col],
                    'Imputed/Encoded': c
                })
        
    elif key == "E":
        # Find differences
        changes_mask = (tmp != df)
        # Create a DataFrame to log changes
        changes = []
        # Iterate through the DataFrame to log changes
        for col in changes_mask.columns:
            for index in changes_mask.index:
                if changes_mask.at[index, col]:  # If there's a change
                    c=df.at[index, col]
                    changes.append({
                    'Column': col,
                    'Original': tmp.at[index, col],
                    'Imputed/Encoded': c
                })
    else:
        print("Wrong Key. Choose either I for Imputation or E for Encoding")
    changes_df = pd.DataFrame(changes).drop_duplicates()
    changes_df.set_index('Column', inplace=True)
    lookup_table = pd.concat([lookup_table, changes_df], axis=0).drop_duplicates()
    return 
  
def simple_cleaning(df):
    print("Performing Simple Cleaning....\n")
    # removing spaces
    for col in df.columns:
        df['Loan Status'] = df['Loan Status'].str.strip()
    
    #Converting to upper case
    df['Addr State'] = df['Addr State'].str.upper()
    df['State'] = df['State'].str.upper()
    
    #Converting to lower case
    df['Purpose'] = df['Purpose'].str.lower()
    df['Type'] = df['Type'].str.lower()
    return df

"1-DATA CLeaning"

def data_cleaning(df):
    print("Performing Data Cleaning.... \n")
    #indexing
    df.set_index('Loan Id', inplace=True)
    
    #Inconsistant
    df_nodesc=df.drop(columns=['Description'])
    # Replacing joint app with joint since they are the same.
    df['Type']=df['Type'].replace('joint app','joint')
    df['Home Ownership']=df['Home Ownership'].replace('ANY','OTHER')
    
    #Irrelevant
    df=df.drop(columns=['Customer Id','Description','Funded Amount','Addr State','Zip Code'])
    
    #Messing Data
    tmp=df.copy()
    df['Annual Inc Joint'].fillna(df['Annual Inc'],inplace=True)
    log_to_lookup_table(tmp[['Annual Inc Joint']],df[['Annual Inc Joint']],'I',"From Annual Inc")
    df=df.rename(columns={'Annual Inc Joint': 'Individual/Joint Annual Inc'})
    
    tmp=df.copy()
    df['Emp Title']=df['Emp Title'].fillna('Not Specified')
    df['Emp Length']=df['Emp Length'].fillna('Not Specified')
    
    df['Int Rate']=df['Int Rate'].fillna(df['Int Rate'].median())
    log_to_lookup_table(tmp,df,"I","median")
    return df

#Outliers
def handel_Outliers(df):
    print("Handeling Outliers....\n")
    numeric_df=df.select_dtypes(include=['float64', 'int64'])
    Outliers=numeric_df.drop('Grade', axis=1).columns.tolist()
    for col in Outliers:
        print(col," : \n")
        # first get the IQR value then apply the rule
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        print(f"IQR = {IQR}")
        # Anything above the upper limit or lower than the lower limit is an outlier
        cut_off = IQR * 1.5
        lower = Q1 - cut_off
        upper =  Q3 + cut_off
        print(f"lower,upper bounds [{lower},{upper}]")
        
        u = df[df[col]> upper]
        l = df[df[col] < lower]
        print(F'Total number of outliers in {col} are {u.shape[0]+ l.shape[0]}')
        
        df.loc[df[col] < lower, col] = df[col].median()
        df.loc[df[col] > upper, col] = df[col].median()

        print("===========================================")
    return df


"2-DATA Transformation"
def feature_engineering(df):
    print("Performoing Feture Engineering.....\n")
    #Feature Engineering
    
    #Issue Month
    df['Issue Date']=pd.to_datetime(df['Issue Date'])
    col_position = df.columns.get_loc('Issue Date')
    df.insert(col_position + 1, 'Issue Day', df['Issue Date'].dt.day)
    df.insert(col_position + 2, 'Issue Month', df['Issue Date'].dt.month)
    df.insert(col_position + 3, 'Issue Year', df['Issue Date'].dt.year)
    df.drop(columns=['Issue Date'],inplace=True)
    
    #Letter Grade
    bins = [0, 5, 10, 15, 20, 25, 30, 35]  # Bin edges
    labels = ['A', 'B', 'C', 'D', 'E', 'F', 'G']  # Corresponding letter grades
    df['Letter Grade'] = pd.cut(df['Grade'], bins=bins, labels=labels, right=True)
    
    #Installment per month
    tmpdf = pd.DataFrame()
    tmpdf['r'] = df['Int Rate'] / 12 
    # Extract the number of months from 'Term'
    tmpdf['n'] = df['Term'].str.extract(r'(\d+)').astype(int)
    # Calculate the monthly payment using the provided formula
    df['Monthly Installment'] = df['Loan Amount'] * (tmpdf['r'] * (1 + tmpdf['r']) ** tmpdf['n']) / ((1 + tmpdf['r']) ** tmpdf['n'] - 1)
    
    #Salary Can Cover
    salary=df['Individual/Joint Annual Inc']/12
    df['Salary Can Cover']=df['Annual Inc'] >= df['Monthly Installment']
    return df

def Encode(df):
    print("Encoding......\n")
    #Encoding
    #Label encoding for ordinal columns
    label_encoder = LabelEncoder()
    df_encoded=df.copy()
    df_encoded['Emp Length'] = label_encoder.fit_transform(df_encoded['Emp Length'])
    df_encoded['Letter Grade'] = label_encoder.fit_transform(df_encoded['Letter Grade'])
    
    #Special case for column Term
    df_encoded['Term'] = df['Term'].str.extract(r'(\d+)').astype(int)
    
    # One-Hot Label encoding for nominal columns
    purpose_counts=df['Purpose'].value_counts()/df.shape[0]*100
    categories_below_1_percent = purpose_counts[purpose_counts < 1].index
    
    # Step 3: Replace entries in the 'Purpose' column that are below 1% with 'Other'
    df_encoded['Purpose'] = df['Purpose'].replace(categories_below_1_percent, 'other')
    df_encoded['Purpose'].value_counts()/df.shape[0]*100
    log_to_lookup_table(df[['Emp Length','Letter Grade','Term','Purpose']],df_encoded[['Emp Length','Letter Grade','Term','Purpose']],"E","")
    df_encoded=pd.get_dummies(df_encoded, columns=['Home Ownership','Verification Status','Loan Status','Type','Purpose'], drop_first=False)
    df_encoded = df_encoded.apply(lambda col: col.astype(int) if col.dtype == 'bool' else col)
    
    print(f"No columns changed from {df.shape[1]} ---> {df_encoded.shape[1]}")
    # Label Encoding for state
    df_encoded['State'] = label_encoder.fit_transform(df['State'])
    log_to_lookup_table(df[['State']],df_encoded[['State']],"E","")
    return df_encoded


def Scale(df_encoded):
    print("Scaling.....\n")
    #Scaling
    columns_to_scale =  ['Annual Inc' , 'Avg Cur Bal' , 'Tot Cur Bal' , 'Loan Amount' , 'Monthly Installment']
    df_scaled=df_encoded.copy()
    df_scaled[columns_to_scale] = StandardScaler().fit_transform(df_scaled[columns_to_scale])
    df_scaled.describe()
    return df_scaled

def final_cleaning(df_scaled):
    #Final Cleaning
    print("Final cleaning ....\n")
    df_final=df_scaled.drop(['Grade','Emp Title'],axis=1)
    print(f"Final shape {df_final.shape}")
    return df_final

def Save_dfs(df_final,lookup_table):
    print("Saving.....\n")
    df_final.to_csv("Data/Fintech_loans_clean.csv")
    lookup_table.to_csv("Data/Lookup_table.csv")
    return

def get_lookup():
    return lookup_table
