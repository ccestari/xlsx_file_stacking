import boto3
import pandas as pd
import os
import io
from io import StringIO

# To be moved to secrets manager for production
access_key_id = secret_access_key_id
access_key = secret_access_key


### Create an S3 client and resource
s3_client = boto3.client(service_name = 's3', region_name = 'aws-region', aws_access_key_id = access_key_id, aws_secret_access_key = access_key)
s3_bucket = boto3.resource(service_name = 's3', region_name = 'aws-region', aws_access_key_id = access_key_id, aws_secret_access_key = access_key)



### Gets file from S3 location based on name, then creates dataframe from usable sections
def get_some_file_from_s3():
	## Bucket and Key are hardcoded - boto requires bucket be string value, not variable standing in for string
	## Key string to be replaced by variable when working iterator is added that can properly identify key from get_object return packet
    the_file_location = s3_client.get_object(Bucket = 'generic-bucket-name', Key = 'bucket-name/FileName.xlsx')
    file_raw = the_file_location['Body'].read()
    file_df = pd.read_excel(io.BytesIO(file_raw), sheet_name = 'Excel Sheet Name', usecols = "A:ZZ", skiprows = 10)	

    return(file_df)


original_df = get_some_file_from_s3()

### Transforms DataFrame from get_comscore into shape.  First drops columns and sets index, then stacks data on index and finally, writes to csv
drop_columns_df = original_df.drop(['Column 1', 'Col 2', 'Column 3'], axis = 1).where((original_df['Some Other Column'].str.contains('Search Term') == False )|(original_df_df['Other Column 3'].isnull == True)).set_index(['Keeping Column', 'Also Keeping COlumn'])
df_stack = drop_columns_df.stack()

def upload_to_s3(stack_file):
    # Renaming file to be done at later time
    csv_buffer = StringIO()
    stack_file.to_csv(csv_buffer)
    stack_bytes = bytes(stack_file)
    s3_client.put_object(Body = stack_bytes, Bucket = 'new-bucket-location1', 'new-bucket-name/New_File_Name.csv')

upload_to_s3(stack_file = df_stack)
