# You can create more variables according to your project. The following are the basic variables that have been provided to you
import pandas as pd
DB_PATH ='/home/dags/Lead_scoring_data_pipeline'
DB_FILE_NAME ='/utils_output.db'

DATA_DIRECTORY = '/home/dags/Lead_scoring_data_pipeline/data'
DATA_DIRECTORY_CLEAN = '/home/dags/Lead_scoring_data_pipeline/data'
INTERACTION_MAPPING ='/mapping/interaction_mapping.csv'
INDEX_COLUMNS = ['first_platform_c','first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag','city_tier']

interaction_mapping_file=pd.read_csv('/home/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv')

INPUT_FILE_NAME= '/leadscoring.csv'
MODEL_INPUT_FILE_NAME='/cleaned_data.csv'

LEAD_SCORING_DB_FILE_NAME='/lead_scoring_data_cleaning.db' 

UNIT_TEST_DB_FILE_NAME = '/unit_test_cases.db'





