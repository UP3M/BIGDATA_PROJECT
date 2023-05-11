import subprocess
command = 'pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org gdown'
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)

file_id = '1-QQylhU5U5rz2N7OP9ddDq0K_mqwrbwv'
output_file = 'data/bigdataproject.zip'

command = "gdown --id {} -O {}".format(file_id, output_file)
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)

command = 'unzip data/bigdataproject.zip -d data/'
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)
#importing modules
import warnings 
warnings.filterwarnings('ignore')
import time
t = time.time()
print('Importing startred...')
import pandas as pd
from datetime import datetime
print('Done, All the required modules are imported. Time elapsed: {}sec'.format(time.time()-t))
# loading data
customer_df = pd.read_csv('data/customers.csv', delimiter = ',', encoding = 'utf-8')
pings_df = pd.read_csv('data/pings.csv', delimiter = ',', encoding = 'utf-8')
test_df = pd.read_csv('data/test.csv', delimiter = ',', encoding = 'utf-8')
keep_id = customer_df['id'][:100]
# create a boolean mask for rows that have id in the keep_ids list
mask = customer_df['id'].isin(keep_id)
mask1 = pings_df['id'].isin(keep_id)
mask2 =  test_df['id'].isin(keep_id)
# filter the DataFrame using the mask
#customer_df = customer_df[mask]
#pings_df = pings_df[mask1]
#test_df = test_df[mask2]
customer_df.drop_duplicates(subset='id', inplace=True)
#customer_df = customer_df.head(100)
#pings_df = pings_df.head(100)
#test_df = test_df.head(100)
#pings_df.drop_duplicates(subset='id', inplace=True)
test_df.to_csv('data/test_df.csv', index=False)
pings_df.to_csv('data/pings_df.csv', index=False)
customer_df.to_csv('data/customer_df.csv', index=False)


