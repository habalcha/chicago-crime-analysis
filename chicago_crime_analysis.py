import pandas as pd
import numpy as np
import psycopg2 
import matplotlib.pyplot as plt
import seaborn as sns

# read the crime data and remove unnecessary columns
def read_crime_file():
    chi_h = pd.read_csv('Homicides.csv')
    chi_c = pd.read_csv('Chicago_Crimes_2012_to_2017.csv')
    chi_c = chi_c.iloc[:, 1:]
    chi_c = chi_c.iloc[:, :-1]
    chi_h = chi_h.iloc[:, :-1]
    
    return chi_h, chi_c

# rename column, stack chi_h and chi_c, write combined file
def process_crime_files():
    chi_h, chi_c = read_crime_file()
    dfchi = pd.concat([chi_h, chi_c], axis = 0)
    
    dfchi.columns = ['id', 'case_num','crime_date','block','iucr', 'crime_type',
                     'description', 'location_description', 'arrest', 'domestic',
                     'beat', 'district', 'ward', 'community_area', 'fbi_code',
                     'x_cord', 'y_cord', 'year', 'updated_on', 'latitude', 'longitude']
    dfchi = dfchi.loc[(dfchi.district != 31)] # district 31 is not part of the Chicago Police district. It seems to be an error with district 13. We decided to remove it. 
    dfchi.to_csv('Final_Chicago_Crime_File.tdf', sep = '\t', index = False)   
    return dfchi
    
    
# combine homicide and crime dataframes, remove unnecessary column from chi_c
def load_pandas():
    dfchi = process_crime_files()
    return dfchi

# combine homicide and crime dataframes, remove unnecessary column from chi_c    
def load_sql():
    # read and process files
    process_crime_files()
    
    # set up SQL connection
    CONNECTION_STRING = ##### add connection string to postreg #####
    SQLConn = psycopg2.connect(CONNECTION_STRING)
    SQLCursor = SQLConn.cursor()
    schema_name = 'chi_crime'
    table_name = 'crime_data'
    # check if the table exists or not. If it does, drop the table. 
    try:        
        SQLCursor.execute("""DROP TABLE %s.%s;""" % (schema_name, table_name))
        SQLConn.commit()    
    except psycopg2.ProgrammingError:
        print("CAUTION: Tablenames not found: %s.%s" % (schema_name, table_name))
        SQLConn.rollback()
        
    # Create a new table and load data into it    
    SQLCursor = SQLConn.cursor() 
    SQLCursor.execute("""
        CREATE TABLE %s.%s 
        (id int            
         , case_num varchar(16) 
         , crime_date date            
         , block varchar(128)            
         , iucr varchar(8) 
         , crime_type varchar(128)
         , description varchar(128)
         , location_description varchar(128)
         , arrest boolean
         , domestic boolean
         , beat int
         , district float
         , ward float
         , community_area float
         , fbi_code varchar(8)
         , x_cord float
         , y_cord float
         , year int
         , updated_on date
         , latitude float
         , longitude float
         );""" % (schema_name, table_name))
    SQLConn.commit()
    SQL_STATEMENT = f"""        
    COPY {schema_name}.{table_name} FROM STDIN WITH
            CSV             
            HEADER            
            DELIMITER AS E'\t';
    """    
    # copy data from our file into the empty sql table we created above
    SQLCursor.copy_expert(sql=SQL_STATEMENT , file=open('Final_Chicago_Crime_File.tdf', 'r'))
    SQLConn.commit()
#    SQLCursor.execute("""GRANT ALL on %s.%s to STUDENTS;"""% (schema_name, table_name))
#    SQLConn.commit()
    
# Test to see if the table we get from load_sql() matches the data in the file we have.   
def tests():
    CONNECTION_STRING = ##### add connection string to postreg #####
    DF = pd.read_csv('Final_Chicago_Crime_File.tdf', sep='\t')
    SQLConn = psycopg2.connect(CONNECTION_STRING)
    SQLCursor = SQLConn.cursor()
    # This file tests our data to make sure that it matches.
    
    # Test #1: Checking the size of the data
    SQLCursor.execute("""Select count(*) from chi_crime.project1;""")
    sql_rows =SQLCursor.fetchall()
    sql_rows = sql_rows[0][0]
    
    DF_rows = DF.shape[0]
    
    assert DF_rows == sql_rows    
    
    # Test #2: Checking the values of an important columns
    SQLCursor.execute("""SELECT year, count(1) as ct from chi_crime.project1 group by 1;""")
    ### Make sure that the order is alphabetical (lists of tuples are sorted by first element)
    sql_rows = SQLCursor.fetchall()
    sql_rows = pd.DataFrame(sql_rows, columns=['year', 'ct']).sort_values(['year'], ascending = True).reset_index(drop=True)
    
    DF_rows = DF.year.value_counts().to_frame().reset_index().rename(columns={'year' : 'ct', 'index' : 'year'}).sort_values(['year'], ascending=True).reset_index(drop=True)
        
    assert DF_rows.equals(sql_rows)

### Does the amount/type stolen impact the likelihood of getting caught: theft
def chicago_theft_crimes_and_arrests(df):
    
    df_theft = df.copy()
    df_theft = df_theft.loc[df_theft.crime_type == 'THEFT']
    df_theft_arrested = df_theft.copy()
    
    df_theft = df_theft.loc[:, ['description', 'arrest']].groupby(['description']).agg({'description':['count']})
    df_theft['Theft_count'] = df_theft['description', 'count']
    df_theft = df_theft.loc[:,['Theft_count']]
    
    df_theft_arrested = df_theft_arrested.loc[df_theft_arrested.arrest == True].groupby(['description']).agg({'description':['count']})
    df_theft_arrested['arrest_count'] = df_theft_arrested['description', 'count']
    df_theft_arrested = df_theft_arrested.loc[:, ['arrest_count']]
    
    df_theft_final = df_theft.merge(df_theft_arrested, on = 'description').sort_values('Theft_count', ascending = False)
    df_theft_final = df_theft_final.iloc[:-3, :]
    
    # plot figure
    plt.figure(figsize = (25, 25))
    p1 = plt.bar(df_theft_final.index, df_theft_final.Theft_count, label = 'Not Arrested')
    p2 = plt.bar(df_theft_final.index, df_theft_final.arrest_count, label = 'Arrested')
    plt.ylabel('Theft Type\n', fontsize = 30)
    plt.xlabel('\nCount', fontsize = 30)
    plt.legend(loc = 1, prop={'size': 30})
    plt.xticks(rotation=90, fontsize = 25)
    plt.yticks(fontsize = 25)
    plt.subplots_adjust(bottom=0.35)
    # name and save figure
    plt.title('Most Frequent Theft Crimes in Chicago and Their Arrests\n', fontsize = 35)
    plt.savefig('Chicago_Theft_Crimes_and_Arrests_1.png')

### Does the area of homicide affect arrest
    
def chicago_homicide_crimes_and_arrests_by_district(df):
    df_homicide = df.copy()
    df_homicide = df_homicide.loc[df_homicide.crime_type == 'HOMICIDE']
    df_homicide_a = df_homicide.copy()
    df_homicide_na = df_homicide.copy()
    
    df_homicide_a = df_homicide_a.loc[:, ['district','description', 'arrest']].groupby(['district', 'description']).agg({'description':['count']})                           
    df_homicide_a['crime_count'] = df_homicide_a['description', 'count']
    df_homicide_a = df_homicide_a.loc[:, ['crime_count']].sort_values('crime_count')
    #
    df_homicide_na = df_homicide_na.loc[df_homicide_na.arrest == True].groupby(['district', 'description']).agg({'description':['count']})
    df_homicide_na['arrest_count'] = df_homicide_na['description', 'count']
    df_homicide_na = df_homicide_na.loc[:, ['arrest_count']].sort_values('arrest_count')
    
    df_homicide_final = df_homicide_a.merge(df_homicide_na, how = 'outer', on = ('district', 'description')).reset_index().sort_values('district')
    
    x_label = df_homicide_final.district.unique()
    
    # plot figure
    plt.figure(figsize = (25, 15))
    plt.bar(df_homicide_final.district, df_homicide_final.crime_count, label = 'Not Arrested')
    plt.bar(df_homicide_final.district, df_homicide_final.arrest_count, label = 'Arrested')
    plt.xlabel('\nDistrict', fontsize = 30)
    plt.ylabel('Crime Count\n', fontsize = 30)
    plt.xticks(x_label, fontsize = 25)
    plt.yticks(fontsize = 25)
    plt.legend(loc = 1, prop={'size': 20})
    
    # name and save figure
    plt.title('Chicago Homicide Crimes and Arrests by District\n', fontsize = 35)
    plt.savefig('Chicago_Homicide_Crimes_and_Arrests_by_District.png')

# Frequency of all crime types in Chicago
def chicago_crime_type_count(df):
    chi_crime_count = df.copy()
    
    chi_crime_count['counter']=1
    x=chi_crime_count['crime_type'].value_counts()
    y = pd.DataFrame(chi_crime_count.groupby('crime_type')['counter'].sum())
    y['crime_type'] = y.index
    y = y.sort_values("counter", ascending = False)
    
    # plot figure
#    sns.set(style='whitegrid',palette ='muted')
    plt.figure(figsize = (25, 30))
    plt.bar(y['crime_type'], y['counter'])
    plt.xlabel('\nCrime Type', fontsize = 30)
    plt.ylabel('Crime Count\n', fontsize = 30)
    plt.yticks(fontsize = 25)
    plt.xticks(rotation=90, fontsize = 25)
    plt.subplots_adjust(bottom=0.5)
    
    # name and save figure
    plt.title('Chicago Crime Type Count\n', fontsize = 35)
    plt.savefig('Chicago_Crime_Type_Count.png')

#Number of thefts per district

def number_of_thefts_per_district(df):
    theft_by_dist=df.copy()
    theft_by_dist = theft_by_dist.loc[(theft_by_dist.district != 31)]
    # filter data 
    theft_by_dist=theft_by_dist.loc[(theft_by_dist.crime_type=='THEFT'),['crime_type', 'district']].groupby('district', as_index=False).agg({'crime_type': ['count']})
    x=theft_by_dist[str('district')]
    y=theft_by_dist[('crime_type', 'count')]
    
    x_label = theft_by_dist.district.unique()
    
    # plot figure
    plt.figure(figsize = (25, 15))
    plt.bar(x, y)
    plt.xticks(x_label, fontsize = 25)
    plt.yticks(fontsize = 25)
    plt.xlabel('\nDistrict', fontsize = 30)
    plt.ylabel('Number of Thefts\n', fontsize = 30)
    
    # name and save figure
    plt.title('Number of Thefts Per District in Chicago\n', fontsize = 35)
    plt.savefig('Number_of_Thefts_Per_District_in_Chicago.png')

#number of arrests per district
def number_of_arrests_for_theft_crime_by_district(df):    
    theft_arrest_by_dist=df.copy()
    theft_arrest_by_dist = theft_arrest_by_dist.loc[(theft_arrest_by_dist.district != 31)]
    theft_arrest_by_dist=theft_arrest_by_dist.loc[((theft_arrest_by_dist.crime_type=='THEFT') & (theft_arrest_by_dist.arrest==True)),['crime_type', 'district', 'arrest']].groupby('district', as_index=False).agg({'arrest': ['count']})
    a=theft_arrest_by_dist[str('district')]
    b=theft_arrest_by_dist[('arrest', 'count')]
    
    x_label = theft_arrest_by_dist.district.unique()
    # plot figure
    plt.figure(figsize = (25, 15))
    plt.bar(a, b)
    plt.xlabel('\nDistrict', fontsize = 30)
    plt.ylabel('Number of Arrests for Theft Crimes\n', fontsize = 30)
    plt.xticks(x_label, fontsize = 25)
    plt.yticks(fontsize = 25)
    
    # name and save figure
    plt.title('Number of Arrests for Theft Crimes Per District\n', fontsize = 35)
    plt.savefig('Number_of_Arrests_for_Theft_Crimes_Per_District.png')


def highest_arrest_rate_by_crime(df):
    arrest_df = df.copy()
    arrest_df = arrest_df.loc[arrest_df.arrest == True].groupby('crime_type', as_index = False).agg({'arrest':'count'})
    
    crime_count_df = df.copy()
    crime_count_df = crime_count_df.groupby('crime_type').agg({'crime_type': 'count'})
    crime_count_df.columns = ['crime_count']
    
    crime_and_arrest = crime_count_df.merge(arrest_df, on = 'crime_type')
    
    crime_and_arrest['crime_rate_in_%'] = crime_and_arrest['crime_count']/df.shape[0]
    
    crime_and_arrest['crime_arrest_rate_in_%'] = crime_and_arrest['arrest']/df.shape[0]
    
    crime_and_arrest['arrest_rate'] = crime_and_arrest['crime_arrest_rate_in_%']/crime_and_arrest['crime_rate_in_%'] 
    crime_and_arrest = crime_and_arrest.sort_values('arrest_rate', ascending = True)
    
    crime_and_arrest = crime_and_arrest.loc[crime_and_arrest['crime_rate_in_%'] > 0.00075]
    
    highest_crime_and_arrest = crime_and_arrest.iloc[:10, :]
    
    plt.figure(figsize = (25, 25))
    plt.bar(highest_crime_and_arrest.crime_type, highest_crime_and_arrest['crime_rate_in_%']*100, label = 'Not Arrested')
    p2 = plt.bar(highest_crime_and_arrest.crime_type, highest_crime_and_arrest['crime_arrest_rate_in_%']*100, label = 'Arrested')
    plt.xlabel('\nCrime Type', fontsize = 30)
    plt.ylabel('Percentage From All Crimes\n', fontsize = 30)
    plt.xticks(rotation=90, fontsize = 25)
    plt.yticks(fontsize = 25)
    plt.legend(loc = 1, prop={'size': 20})
    plt.subplots_adjust(bottom=0.4)
    plt.title('Crimes With the 10 Highest Arrest Rate\n', fontsize = 35)
    plt.savefig('Crimes_With_the_10_Highest_Arrest_Rate.png')
      
def lowest_arrest_rate_by_crime(df):
    arrest_df = df.copy()
    arrest_df = arrest_df.loc[arrest_df.arrest == True].groupby('crime_type', as_index = False).agg({'arrest':'count'})
    
    crime_count_df = df.copy()
    crime_count_df = crime_count_df.groupby('crime_type').agg({'crime_type': 'count'})
    crime_count_df.columns = ['crime_count']
    
    crime_and_arrest = crime_count_df.merge(arrest_df, on = 'crime_type')
    
    crime_and_arrest['crime_rate_in_%'] = crime_and_arrest['crime_count']/df.shape[0]
    
    crime_and_arrest['crime_arrest_rate_in_%'] = crime_and_arrest['arrest']/df.shape[0]
    
    crime_and_arrest['arrest_rate'] = crime_and_arrest['crime_arrest_rate_in_%']/crime_and_arrest['crime_rate_in_%'] 
    crime_and_arrest = crime_and_arrest.sort_values('arrest_rate', ascending = True)
    
    crime_and_arrest = crime_and_arrest.loc[crime_and_arrest['crime_rate_in_%'] > 0.00075]
    
    lowest_crime_and_arrest = crime_and_arrest.iloc[:10, :]
    
    plt.figure(figsize = (25, 25))
    plt.bar(lowest_crime_and_arrest.crime_type, lowest_crime_and_arrest['crime_rate_in_%']*100, label = 'Not Arrested')
    plt.bar(lowest_crime_and_arrest.crime_type, lowest_crime_and_arrest['crime_arrest_rate_in_%']*100, label = 'Arrested')
    
    plt.xlabel('\nCrime Type', fontsize = 30)
    plt.ylabel('Percentage From All Crimes\n', fontsize = 30)
    plt.xticks(rotation=90, fontsize = 25)
    plt.yticks(fontsize = 25)
    plt.legend(loc = 1, prop={'size': 20})
    plt.title('Crimes With the 10 Lowest Arrest Rate\n', fontsize = 35)
    plt.subplots_adjust(bottom=0.3)
    plt.savefig('Crimes_With_the_10_Lowest_Arrest_Rate.png')
    
def chicago_district_arrest_chi_squared_test():
    district18arrests=3752
    district18thefts=30287
    
    district1arrests=5113
    district1thefts=29523
    
    district19arrests=2223
    district19thefts=23739
    
    district12arrests=1848
    district12thefts=21526
    
    district8arrests=2659
    district8thefts=19980
    
    n=143650
    total_thefts=125055
    total_arrests=15595
    
    ##now calculate the expected value for each category
    
    ##district 18
    dis18theftsEx=((district18arrests + district18thefts) * total_thefts)/n
    dis18arrestsEx=((district18arrests + district18thefts) * total_arrests)/n
    
    ##district 1
    dis1theftsEx=((district1arrests + district1thefts) * total_thefts)/n
    dis1arrestsEx=((district1arrests + district1thefts) * total_arrests)/n
    
    ##district 19
    dis19theftsEx=((district19arrests + district19thefts) * total_thefts)/n
    dis19arrestsEx=((district19arrests + district19thefts) * total_arrests)/n
    
    ##district 12 
    dis12theftsEx=((district12arrests + district12thefts) * total_thefts)/n
    dis12arrestsEx=((district12arrests + district12thefts) * total_arrests)/n
    
    ##district 8
    dis8theftsEx=((district8arrests + district8thefts) * total_thefts)/n
    dis8arrestsEx=((district8arrests + district8thefts) * total_arrests)/n
    
    ##now we need to calculate the test statistic using the differece of squares method 
    
    qteststat=((district18thefts - dis18theftsEx)**2/dis18theftsEx) + ((district18arrests - dis18arrestsEx)**2/dis18arrestsEx) + ((district1thefts - dis1theftsEx)**2/dis1theftsEx) + ((district1arrests - dis1arrestsEx)**2/dis1arrestsEx) + ((district19thefts - dis19theftsEx)**2/dis19theftsEx) + ((district19arrests - dis19arrestsEx)**2/dis19arrestsEx) + ((district12thefts - dis12theftsEx)**2/dis12theftsEx) + ((district12arrests - dis12arrestsEx)**2/dis12arrestsEx) +((district8thefts - dis8theftsEx)**2/dis8theftsEx) + ((district8arrests - dis8arrestsEx)**2/dis8arrestsEx) 
    
    ##now we need to find the critical region 
    ##we will use a significance level of .05
    ##we will use the chi-square table to find the value critcal value with degress of freedom 4 
    
    criticalRegion= 9.88 
    
    if (qteststat>criticalRegion):
            print ("We reject Ho, and accept Hi. Hence the two variables are not independent")
    else: 
            print ("We fail to reject Ho, hence the two variables are independent")
            
#%%
       
#load_sql()
#%%
CONNECTION_STRING = ##### add connection string to postreg #####
SQLConn = psycopg2.connect(CONNECTION_STRING)
SQLCursor = SQLConn.cursor()

SQLCursor.execute("""Select * from chi_crime.project1;""")
chi_crime_df =SQLCursor.fetchall()

chi_crime = pd.DataFrame(chi_crime_df)
chi_crime.columns = ['id', 'case_num','crime_date','block','iucr', 'crime_type',
                     'description', 'location_description', 'arrest', 'domestic',
                     'beat', 'district', 'ward', 'community_area', 'fbi_code',
                     'x_cord', 'y_cord', 'year', 'updated_on', 'latitude', 'longitude']

#%%
#chi_crime_narcotics = chi_crime.loc[chi_crime.crime_type == "NARCOTICS"].groupby("description").agg({"description":"count"})
#chi_crime_narcotics.columns = ["count"]
#chi_crime_narcotics = chi_crime_narcotics.sort_values("count", ascending = False)
#
#x = chi_crime_narcotics.sum()/chi_crime.shape[0]
#
#chi_crime_narcotics = chi_crime_narcotics/chi_crime_narcotics.sum()
#chi_crime_narcotics = chi_crime_narcotics * 100

#chi_crime_narcotics = chi_crime.loc[chi_crime.crime_type == "NARCOTICS"].groupby("district").agg({"district":"count"})
#chi_crime_narcotics.columns = ["count"]
#chi_crime_narcotics = chi_crime_narcotics.sort_values("count", ascending = False)

#%%
#chi_crime_narcotics = chi_crime.loc[chi_crime.crime_type == "NARCOTICS"].groupby("crime_type").agg({"crime_type":"count"})

#%%
chicago_homicide_crimes_and_arrests_by_district(chi_crime)
#%%
number_of_thefts_per_district(chi_crime)
#%%
number_of_arrests_for_theft_crime_by_district(chi_crime)
#%%
lowest_arrest_rate_by_crime(chi_crime)
#%%
highest_arrest_rate_by_crime(chi_crime)
#%%
chicago_crime_type_count(chi_crime)
#%%
chicago_theft_crimes_and_arrests(chi_crime)
#%%
chicago_district_arrest_chi_squared_test()






