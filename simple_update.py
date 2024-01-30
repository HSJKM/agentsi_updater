from dotenv import load_dotenv
import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import requests
import json
import datetime as dt

load_dotenv() 

# Establish a connection to the MySQL server
cnx = mysql.connector.connect(user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
                              host=os.getenv('DB_HOST'),
                              database=os.getenv('DB_DATABASE'))


# Use pandas to execute a SQL query and download the data into a DataFrame
df_matched = pd.read_sql('''
                SELECT *
                FROM v_listings_v3_1
                WHERE (listed_at = CURDATE() - INTERVAL 5 DAY
                 OR underoffer_at = CURDATE() - INTERVAL 5 DAY
                 OR sstc_at = CURDATE() - INTERVAL 5 DAY
                 OR reduced_at = CURDATE() - INTERVAL 5 DAY) and address_id is not null and district = 'M16'
                ;''', con=cnx)

df_unmatched = pd.read_sql('''
                SELECT *
                FROM v_listings_v3_1
                WHERE (listed_at = CURDATE() - INTERVAL 1 DAY
                 OR underoffer_at = CURDATE() - INTERVAL 1 DAY
                 OR sstc_at = CURDATE() - INTERVAL 1 DAY
                 OR reduced_at = CURDATE() - INTERVAL 1 DAY) and address_id is null
                ;''', con=cnx)

cnx.close()

def status_id_enum(id):
    if id == 1:
        return 'onmarket'
    elif id == 2:
        return 'underoffer'
    elif id == 12:
        return 'sstc'
    

def get_doc_id_ai_id(address_id):
    url = f"{os.getenv('ELASTIC_HOSE')}/agent_sample_unified_search_index/_search"

    # specify the query
    query = {
        "query": {
            "match": {
                "address_id": address_id
            }
        }
    }

    # send a POST request
    response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(query))
    res_json = json.loads(response.text)
    if res_json['hits']['total']['value'] == 0:
        return None
    else:
        return json.loads(response.text)['hits']['hits'][0]['_id']


def insert_updated_details_os(row):
    doc_id = get_doc_id_ai_id(row['address_id'])
    
    if doc_id:
        print(doc_id)
        # specify the url
        url = f"{os.getenv('ELASTIC_HOSE')}/agent_sample_unified_search_index/_update/{doc_id}"


        if row['type'] == 'S':
        # specify the update
            update = {
                "doc": {
                    "sales_event_date": str(dt.date.today()),
                    "sales_act_status": status_id_enum(row['status_id']),
                    "price": row['current_price'],
                    "listing_activity_status": True
                }
            }
        if row['type'] == 'L':
        # specify the update
            update = {
                "doc": {
                    "last_rented": str(dt.date.today()),
                    "last_rented_value": row['current_price'],
                    "listing_activity_status": True
                }
            }
        # send a POST request
        response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(update))

        # print the response
        print(json.loads(response.text))
    else:
        print('no doc id')


for index,row in df_matched.iterrows():
    print(df_matched.loc[index,['address_id', 'status_id']])
    insert_updated_details_os(row)

exit()