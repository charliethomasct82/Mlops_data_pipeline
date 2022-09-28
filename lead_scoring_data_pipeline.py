##############################################################################
# Import necessary modules

import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from significant_categorical_level import list_platform,list_medium,list_source
from city_tier_mapping import *
from constants import *
from utils import *
from data_validation_checks import *
from schema import *
# #############################################################################


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

utils = module_from_file("utils", "/home/dags/Lead_scoring_data_pipeline/utils.py")
#####################################################################################
city_tier_mapping = {'bengaluru': 1,
     'chennai': 1,
     'hyderabad': 1,
     'kolkata': 1,
     'mumbai': 1,
     'ncr': 1,
     'pune': 1,
     'agra': 2,
     'ahmedabad': 2,
     'aligarh': 2,
     'anand': 2,
     'bhopal': 2,
     'coimbatore': 2,
     'gandhinagar': 2,
     'gwalior': 2,
     'indore': 2,
     'jabalpur': 2,
     'jaipur': 2,
     'kanpur': 2,
     'kochi': 2,
     'lucknow': 2,
     'ludhiana': 2,
     'madurai': 2,
     'meerut': 2,
     'nagpur': 2,
     'nashik': 2,
     'patna': 2,
     'rajkot': 2,
     'solapur': 2,
     'surat': 2,
     'tiruchirapalli': 2,
     'vadodara': 2,
     'vijayawada': 2,
     'vishakapatnam': 2,
     'allahabad': 3,
     'amravati': 3,
     'amritsar': 3,
     'aurangabad': 3,
     'bhavnagar': 3,
     'bhilai': 3,
     'bhubaneswar': 3,
     'chandigarh': 3,
     'dehradun': 3,
     'dhanbad': 3,
     'etawah': 3,
     'faridabad': 3,
     'gorakhpur': 3,
     'guntur': 3,
     'guwahati': 3,
     'hajipur': 3,
     'hubli-dharwad': 3,
     'jammu': 3,
     'jamnagar': 3,
     'jamshedpur': 3,
     'jodhpur': 3,
     'kannur': 3,
     'kollam': 3,
     'kozhikode': 3,
     'kurnool': 3,
     'mangalore': 3,
     'mysore': 3,
     'nellore': 3,
     'patiala': 3,
     'pondicherry': 3,
     'raipur': 3,
     'ranchi': 3,
     'roorkee': 3,
     'salem': 3,
     'sangli': 3,
     'srinagar': 3,
     'thiruvananthapuram': 3,
     'thrissur': 3,
     'tirunelveli': 3,
     'varanasi': 3,
     'vellore': 3,
     'warangal': 3}

####################################################################################
# You can create more variables according to your project. The following are the basic variables that have been provided to you
import pandas as pd
DB_PATH ='/home/dags/Lead_scoring_data_pipeline'
DB_FILE_NAME ='/utils_output.db'

DATA_DIRECTORY = '/home/dags/Lead_scoring_data_pipeline/data'
DATA_DIRECTORY_CLEAN = '/home/dags/Lead_scoring_data_pipeline/data'
INTERACTION_MAPPING ='/mapping/interaction_mapping.csv'
INDEX_COLUMNS = ['first_platform_c','first_utm_medium_c', 'first_utm_source_c', 'total_leads_dropped',
       'referred_lead', 'app_complete_flag','city_tier']

interaction_mapping_file=pd.read_csv('/home/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv')

INPUT_FILE_NAME= '/leadscoring.csv'
MODEL_INPUT_FILE_NAME='/cleaned_data.csv'

LEAD_SCORING_DB_FILE_NAME='/lead_scoring_data_cleaning.db' 

UNIT_TEST_DB_FILE_NAME = '/unit_test_cases.db'
##################################################################################
# levels are stored in list for further processing

list_platform=['Level0', 'Level3', 'Level7', 'Level1', 'Level2', 'Level8']

list_medium=['Level0', 'Level2', 'Level6', 'Level3', 'Level4', 'Level9', 'Level11', 'Level5', 'Level8', 'Level20', 'Level13', 'Level30', 'Level33', 'Level16', 'Level10', 'Level15', 'Level26', 'Level43']

list_source=['Level2', 'Level0', 'Level7', 'Level4', 'Level6', 'Level16', 'Level5', 'Level14']
##############################################################################
raw_data_schema = ['created_date', 'city_mapped', 'first_platform_c',
           'first_utm_medium_c', 'first_utm_source_c', 'total_leads_dropped',
           'referred_lead', '1_on_1_industry_mentorship', 'call_us_button_clicked',
           'career_assistance', 'career_coach', 'career_impact', 'careers',
           'chat_clicked', 'companies', 'download_button_clicked',
           'download_syllabus', 'emi_partner_click', 'emi_plans_clicked',
           'fee_component_click', 'hiring_partners',
           'homepage_upgrad_support_number_clicked',
           'industry_projects_case_studies', 'live_chat_button_clicked',
           'payment_amount_toggle_mover', 'placement_support',
           'placement_support_banner_tab_clicked', 'program_structure',
           'programme_curriculum', 'programme_faculty',
           'request_callback_on_instant_customer_support_cta_clicked',
           'shorts_entry_click', 'social_referral_click',
           'specialisation_tab_clicked', 'specializations', 'specilization_click',
           'syllabus', 'syllabus_expand', 'syllabus_submodule_expand',
           'tab_career_assistance', 'tab_job_opportunities', 'tab_student_support',
           'view_programs_page', 'whatsapp_chat_click', 'app_complete_flag']

model_input_schema = ['first_platform_c','first_utm_medium_c','first_utm_source_c','total_leads_dropped','referred_lead', 
                    'app_complete_flag','city_tier']
###############################################################################
# Define the function to build database
###############################################################################



def build_dbs(db_path,db_file_name):
    if os.path.isfile(db_path+db_file_name):
        print( "DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            
            conn = sqlite3.connect(db_path+db_file_name)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"
'''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '   


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
'''
###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check(raw_data_schema):
    df=pd.read_csv(DATA_DIRECTORY + INPUT_FILE_NAME)
    if (df.columns[1:]==raw_data_schema).all():
        return('Raw datas schema is in line with the schema present in schema.py')
    else:
        return('Raw datas schema is NOT in line with the schema present in schema.py')
    
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be   
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''


###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db(DATA_DIRECTORY,DB_PATH,DB_FILE_NAME,INPUT_FILE_NAME):
    df=pd.read_csv(DATA_DIRECTORY + INPUT_FILE_NAME )
    df['total_leads_dropped'].fillna(0, inplace=True)
    df['referred_lead'].fillna(0, inplace=True)
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print("Connection Successful",conn)
    df.to_sql('Loaded_data',con=conn,if_exists='replace',index=False)
    df_read=pd.read_sql('select * from Loaded_data', conn)
    return(df_read.head())

    '''
    This function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier(DB_PATH,city_tier_mapping,DB_FILE_NAME):
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)

    df = pd.read_sql("select * from Loaded_data", con=conn)
    df_lead_scoring=df
    df_lead_scoring["city_tier"] = df_lead_scoring["city_mapped"].map(city_tier_mapping)
    df_lead_scoring["city_tier"] = df_lead_scoring["city_tier"].fillna(3.0)
    df=df_lead_scoring
    
    print("Connection Successful",conn)
    df.to_sql('city_tier_mapped',con=conn,if_exists='replace',index=False)
    df_read=pd.read_sql('select * from city_tier_mapped', conn)
    return(df_read.head())
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in /mappings/city_tier_mapping.py file. If a
    particular city's tier isn't mapped in the city_tier_mapping.py then
    the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars(DB_PATH,DB_FILE_NAME):
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)

    df_lead_scoring= pd.read_sql("select * from city_tier_mapped",con=conn)
   
    #df_lead_scoring=pd.read_csv('C:\\Users\\44775\\Desktop\\Assignment\\01_data_pipeline\\scripts\\data\\leadscoring.csv')
     # cumulative percetage is calculated for all the three columns - first_platform_c, first_utm_medium_c, first_utm_source_c
    for column in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
        df_cat_freq = df_lead_scoring[column].value_counts()
        df_cat_freq = pd.DataFrame({'column':df_cat_freq.index, 'value':df_cat_freq.values})
        df_cat_freq['perc'] = df_cat_freq['value'].cumsum()/df_cat_freq['value'].sum()
        
      
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in      list_platform
    new_df.loc['first_platform_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
    new_df.loc['first_utm_medium_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
    new_df.loc['first_utm_source_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    print("Connection Successful",conn)
    df.to_sql('categorical_variables_mapped',conn,if_exists='replace',index=False)
    df_read=pd.read_sql('select * from categorical_variables_mapped', conn)
    return(df_read.head())

    '''
    This function maps all the unsugnificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping(DB_PATH,interaction_mapping_file,INDEX_COLUMNS,DB_FILE_NAME):
    
    # Read dataframe from the database
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df = pd.read_sql("select * from categorical_variables_mapped", con=conn)
    
    
    # unpivot the interaction columns and put the values in rows
    df_unpivot = pd.melt(df, id_vars=INDEX_COLUMNS, var_name='interaction_type', value_name='interaction_value')
    # handle the nulls in the interaction value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    # map interaction type column with the mapping file to get interaction mapping
    interaction_mapping_file=pd.DataFrame(interaction_mapping_file)
    df = pd.merge(df_unpivot,interaction_mapping_file, on='interaction_type', how='left')
    #dropping the interaction type column as it is not needed
    df = df.drop(['interaction_type'], axis=1)
     # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(
        values='interaction_value', index=INDEX_COLUMNS, columns='interaction_mapping', aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    #df_pivot.drop(['created_date'],axis=1,inplace=True)
   
    
    # generate the profile report of final dataset

    #profile = ProfileReport(df_pivot, title="Cleaned Data Summary")
    #profile.to_notebook_iframe()
    #profile.to_file('profile_report/cleaned_data_report.html')
    # the file is written in the data folder
    print("Connection Successful",conn)
    df_pivot.to_sql('cleaned_data',conn,if_exists='replace',index=False)
    return(df_pivot.head())
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        interaction_mapping_file : path to the csv file containing interaction's
                                   mappings
        index_columns : list of columns to be used as index while pivoting and
                        unpivoting
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our index_columns. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' in it as index_column else pass a list without 'app_complete_flag'
        in it.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check(model_input_schema):
    #conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    #df= pd.read_sql("select * cleaned_data",con=conn)
    df=pd.read_csv(DATA_DIRECTORY_CLEAN+MODEL_INPUT_FILE_NAME)
    if (df.columns==model_input_schema).all():
        print("Connection Successful",conn)
        df.to_sql('cleaned_data',conn,if_exists='replace',index=False)
        return('Raw datas schema is in line with the schema present in schema.py')
    else:
        return('Raw datas schema is NOT in line with the schema present in schema.py')
    

###############################################################################
# Define default arguments and DAG
###############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


dags = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
###############################################################################
op_create_db = PythonOperator(task_id='building_db', 
                            python_callable=utils.build_dbs,
                            op_kwargs={'db_path': DB_PATH, 'db_file_name': LEAD_SCORING_DB_FILE_NAME},
                            dag=dags)

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
##############################################################################
op_load_data = PythonOperator(task_id='loading_data', 
                            python_callable=utils.load_data_into_db,
                              op_kwargs={ 'DATA_DIRECTORY':DATA_DIRECTORY,'DB_PATH': DB_PATH, 'DB_FILE_NAME': LEAD_SCORING_DB_FILE_NAME,'INPUT_FILE_NAME':INPUT_FILE_NAME},
                            dag=dags)
###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
###############################################################################
op_map_city_tier = PythonOperator(task_id='map_city', 
                            python_callable=utils.map_city_tier,
                            op_kwargs={'DB_PATH': DB_PATH,'city_tier_mapping':city_tier_mapping, 'DB_FILE_NAME': LEAD_SCORING_DB_FILE_NAME},
                            dag=dags)
###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
###############################################################################
op_map_categorical_vars = PythonOperator(task_id='mapping_categorical_vars', 
                            python_callable=utils.map_categorical_vars,
                            op_kwargs={'DB_PATH': DB_PATH, 'DB_FILE_NAME': LEAD_SCORING_DB_FILE_NAME},
                            dag=dags)
###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
###############################################################################
op_interactions_mapping = PythonOperator(task_id='mapping_interactions', 
                            python_callable=utils.interactions_mapping,
                            op_kwargs={'DB_PATH': DB_PATH,'interaction_mapping_file':interaction_mapping_file,'INDEX_COLUMNS':INDEX_COLUMNS, 'DB_FILE_NAME': LEAD_SCORING_DB_FILE_NAME},
                            dag=dags)

###############################################################################
# Define the relation between the tasks
###############################################################################

# op_init.set_downstream(op_create_db)
op_create_db.set_downstream(op_raw_data_schema_check)

# op_init.set_downstream(op_create_db)
op_raw_data_schema_check.set_downstream(op_load_data)

# op_init.set_downstream(op_create_db)
op_load_data.set_downstream(op_map_city_tier)

# op_init.set_downstream(op_create_db)
op_map_city_tier.set_downstream(op_map_categorical_vars)

# op_init.set_downstream(op_create_db)
op_map_categorical_vars.set_downstream(op_interactions_mapping)

# op_init.set_downstream(op_create_db)
op_interactions_mapping.set_downstream(op_model_input_schema_check)

