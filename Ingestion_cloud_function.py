import pandas as pd
import json
import numpy as np
from google.cloud import bigquery
import hashlib

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
    event (dict): Event payload.
    context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    #print(f"Processing file: {file['name']}.")
    path = file['name']
    INPUT_PATH = f"gs://{file['bucket']}/{path}"
    print(INPUT_PATH)
    #return INPUT_PATH
    if ('customer' in INPUT_PATH):
        OUTPUT_PATH = 'acn-in-cf-data-ggl-aca-c01-t04.test1.final-customer1'
        return ingest_customer_data(INPUT_PATH, OUTPUT_PATH)
    elif ('CDR' in INPUT_PATH):
        OUTPUT_PATH = 'acn-in-cf-data-ggl-aca-c01-t04.test1.final-CDR1'
        return ingest_CDR_data(INPUT_PATH, OUTPUT_PATH)
    elif ('internet' in INPUT_PATH):
        OUTPUT_PATH = 'acn-in-cf-data-ggl-aca-c01-t04.test1.final-internet1'
        return ingest_internet_data(INPUT_PATH, OUTPUT_PATH)
    elif ('transaction' in INPUT_PATH):
        OUTPUT_PATH = 'acn-in-cf-data-ggl-aca-c01-t04.test1.final-trans1'
        return ingest_trans_data(INPUT_PATH, OUTPUT_PATH)
    else:
        return 'ERROR'
    return 'SUCCESS'

def ingest_customer_data(INPUT_PATH, OUTPUT_PATH):
    '''
    Function to Ingest Customer Data from Google Cloud Storage and load it to BigQuery
    '''
    data = pd.read_json(INPUT_PATH, lines=True)
    df = pd.DataFrame()
    customer_name1 = []
    customer_id1 = []
    Gender = []
    zip_code = []
    connection_type = []
    connection_plan = []
    new_ported = []
    old_operator = []
    curr_operator = []
    phone_number = []
    contract_date = []
    SSN = []
    DL = []
    for lab, row in data.iterrows():
        customer_name1.append(row['root']['cus_name'])
        customer_id1.append(row['root']['cusomer_id'])
        Gender.append(row['root']['gender'])
        connection_type.append(row['connection']['type'])
        connection_plan.append(row['connection']['plan'])
        zip_code.append(row['identities']['DL']['address']['zip_code'])
        new_ported.append(row['phone']['new/port']['ported'])
        old_operator.append(row['phone']['new/port']['old_operator'])
        phone_number.append(row['phone']['number'])
        curr_operator.append(row['phone']['operator'])
        contract_date.append(row['phone']['start_date'])
        SSN.append(row['identities']['SSN']['number'])
        DL.append(row['identities']['DL']['number'])
    df['customer_id'] = customer_id1
    df['customer_name'] = customer_name1
    df['phone_number'] = phone_number
    df['zip_code'] = zip_code
    df['Gender'] = Gender
    df['connection_type'] = connection_type
    df['connection_plan'] = connection_plan
    df['new_ported'] = new_ported
    df['old_operator'] = old_operator
    df['curr_operator'] = curr_operator
    df['contract_date'] = contract_date
    df['SSN'] = SSN
    df['DL'] = DL
    df['SSN_hashed'] = df['SSN'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    df['DL_hashed'] = df['DL'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    df.drop(['SSN', 'DL'], axis = 1, inplace = True)
    df.to_gbq(OUTPUT_PATH,  project_id='acn-in-cf-data-ggl-aca-c01-t04', if_exists='replace')
    #to_bq(df, OUTPUT_PATH)
    return "SUCCESS customer data"

def ingest_CDR_data(INPUT_PATH, OUTPUT_PATH):
    '''
    Function to Ingest Call Detail Record Data from Google Cloud Storage and load it to BigQuery
    '''
    data = pd.read_json(INPUT_PATH, lines=True)
    source_no = []
    dest_no = []
    start_time = []
    duration = []
    source_operator = []
    dest_operator = []
    source_location = []
    dest_location = []
    disposition = []
    sms = []
    for lab, row in data.iterrows():
        source_no.append(row['source']['msisdn'])
        dest_no.append(row['destination']['msisdn'])
        start_time.append(row['root']['start_time'])
        duration.append(float(row['root']['duration']))
        source_operator.append(row['source']['operator'])
        dest_operator.append(row['destination']['operator'])
        source_location.append(row['source']['location'])
        dest_location.append(row['destination']['location'])
        disposition.append(row['root']['disposition'])
        sms.append(row['root']['sms'])
    df = pd.DataFrame()
    df['source_no'] = source_no
    df['dest_no'] = dest_no
    df['start_time1'] = start_time
    df['start_time'] = pd.to_datetime(df['start_time1'])
    df.drop('start_time1', axis = 1, inplace = True)
    df['duration'] = duration
    df['source_operator'] = source_operator
    df['dest_operator'] = dest_operator
    df['source_location'] = source_location
    df['dest_location'] = dest_location
    df['disposition'] = disposition
    df['sms'] = sms
    df.replace('nan', np.nan, inplace=True)
    #df.to_gbq(OUTPUT_PATH,  project_id='acn-in-cf-data-ggl-aca-c01-t04', if_exists='append')
    to_bq(df, OUTPUT_PATH)
    return "SUCCESS CDR data"

def ingest_internet_data(INPUT_PATH, OUTPUT_PATH):
    '''
    Function to Ingest Internet Data from Google Cloud Storage and load it to BigQuery
    '''
    data = pd.read_json(INPUT_PATH, lines=True)
    phone_no = []
    source_IP = []
    dest_IP = []
    start_time = []
    source_port = []
    dest_port = []
    ASN = []
    no_of_packets = []
    no_of_bytes = []
    for lab, row in data.iterrows():
        phone_no.append(row['root']['phone_number'])
        source_IP.append(row['source']['ip'])
        dest_IP.append(row['destination']['ip'])
        start_time.append(row['root']['time'])
        source_port.append(row['source']['port'])
        dest_port.append(row['destination']['port'])
        ASN.append(row['root']['asn'])
        no_of_packets.append(float(row['root']['num_packets']))
        no_of_bytes.append(float(row['root']['num_bytes']))
    df = pd.DataFrame()
    df['phone_no'] = phone_no
    df['source_IP'] = source_IP
    df['dest_IP'] = dest_IP
    df['start_time1'] = start_time
    df['start_time'] = pd.to_datetime(df['start_time1'])
    df.drop('start_time1', axis = 1, inplace = True)
    df['source_port'] = source_port
    df['dest_port'] = dest_port
    df['ASN'] = ASN
    df['no_of_packets'] = no_of_packets
    df['no_of_bytes'] = no_of_bytes
    df.replace('nan', np.nan, inplace=True)
    #df.to_gbq(OUTPUT_PATH,  project_id='acn-in-cf-data-ggl-aca-c01-t04', if_exists='append')
    to_bq(df, OUTPUT_PATH)
    return "SUCCESS internet data"

def ingest_trans_data(INPUT_PATH, OUTPUT_PATH):
    '''
    Function to Ingest Transaction Data from Google Cloud Storage and load it to BigQuery
    '''
    data = pd.read_json(INPUT_PATH, lines=True)
    data['transact_date'] = pd.to_datetime(data['transaction_date'])
    data.drop('transaction_date', axis = 1, inplace=True)
    #data.to_gbq(OUTPUT_PATH,  project_id='acn-in-cf-data-ggl-aca-c01-t04', if_exists='append')
    to_bq(data, OUTPUT_PATH)
    return "SUCCESS transaction data"

def to_bq(df, TABLE_ID):
    '''
    Function to load data into BigQuery
    '''
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND', autodetect = True)
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)  # Make an API request.
    #load_job.result()
    #return 'SUCCESS'