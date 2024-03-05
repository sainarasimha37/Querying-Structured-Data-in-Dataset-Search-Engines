
import os
import io
import duckdb
from minio import Minio
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import json
from io import BytesIO
from flask import Flask, render_template

app = Flask(__name__)
CORS(app)

minio_client = Minio(
    "localhost:8050",
    access_key="devkey",
    secret_key="devpassword",
    secure=False
)
duckdb_client = duckdb.connect()
duckdb_client.execute("INSTALL httpfs")
duckdb_client.execute("LOAD httpfs")
duckdb_client.execute("SET s3_endpoint='localhost:8050'")
duckdb_client.execute("SET s3_use_ssl= false")
duckdb_client.execute("SET s3_url_style='path'")
duckdb_client.execute("SET s3_region='us-east-1'")
duckdb_client.execute("SET s3_access_key_id='devkey'")
duckdb_client.execute("SET s3_secret_access_key='devpassword'")

import os

def upload_parquet_to_minio(parquet_path):
    """
    Uploads a Parquet file to MinIO object storage.
    """
    
    with open(parquet_path, 'rb') as file_data:
        object_name = parquet_path.split("/")[-1]
        file_stat = os.stat(parquet_path)
        minio_client.put_object(
            "discovery-bucket",
            object_name,
            file_data,
            length=file_stat.st_size,
            content_type='application/octet-stream'
        )
    print(f"File '{object_name}' uploaded successfully to MinIO!")




def get_parquet_file_from_minio(parquet_file_path):
    """
    Downloads a Parquet file from MinIO object storage and returns the in-memory file object.
    """
    
    parquet_file = io.BytesIO()
    minio_client.get_object(
        "discovery-bucket",
        parquet_file_path,
        parquet_file
    )
    with open("temp.parquet", "wb") as f:
        f.write(parquet_file.getbuffer())

    table = pq.read_table("temp.parquet")
    return table

def download_dataset(dataset_id, format='csv'):
    url = f'https://auctus.vida-nyu.org/api/v1/download/{dataset_id}?format={format}'
    response = requests.get(url)
    if response.ok:
        return response.content
    else:
        raise Exception(f'Failed to download dataset {dataset_id}. Response status code: {response.status_code}')

def convert_csv_to_parquet(csv_path, parquet_path):
    df = pd.read_csv(csv_path)
    table = pa.Table.from_pandas(df)
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    print(f"Writing Parquet file to {parquet_path}")
    pq.write_table(table, parquet_path)
    print(f"Parquet file written to {parquet_path}")
result = None

@app.route('/')
def hello():
    return render_template('sqlquery.html', chart_data=result)
   

@app.route('/query', methods=['GET'])
def handle_query():
    type_ = request.headers.get('Content-Type', '')
    result = None
    data = request.args
    
    try:
        object_name = data['object_name']
        query = data['query']
        try:
            is_object = minio_client.stat_object("nvn", object_name)
        except Exception as err:
            is_object = False
        
        print("Bucket name: %s", "nvn")
        if not is_object:
            print("Object doesn't exist in s3")          
            response =  requests.get("http://localhost:8002/api/v1/download/"+object_name, allow_redirects=True)
            file = BytesIO(response.content)
            df = pd.read_csv(file)
            parquet_file = BytesIO()
            pq.write_table(pa.Table.from_pandas(df), parquet_file)
            parquet_file.seek(0)
            minio_client.put_object("nvn", object_name+".parquet", parquet_file, len(parquet_file.getvalue()), content_type="application/parquet")
            print("Uploaded the parquet file to s3")
        duckdb = duckdb_client
        object_name = object_name+".parquet"
        data = "read_parquet('s3://nvn/"+object_name+"')"
        query = query.replace("data", data)
        result = json.loads(duckdb.execute(query).fetchdf().to_json(orient="table"))
    except Exception as e:
        print(f"Error querying data from S3: {e}")
        return {'results': []}
    
    return {'results': result}if result is not None else {'results': []}


if __name__ == '__main__':
    app.run(debug=True,port=5001)
