
import requests, time
from authlib.jose import jwt
from datetime import date, datetime, timedelta
import gzip
from azure.storage.blob import *
import io
import pandas as pd
import os

# Setup KV connection and load env variables:
secret_PRIVATE_KEY = str(os.environ["AppStoreConnect_PRIVATE_KEY"])
secret_ISSUER_ID = str(os.environ["AppStoreConnect_ISSUER_ID"])
secret_KEY_ID = str(os.environ["AppStoreConnect_KEY_ID"])
secret_CONN_STR = str(os.environ["AppStoreConnect_CONNECTIONSTRING_SQLServer"])

EXPIRATION_TIME = int(round(time.time() + (20.0 * 60.0))) # 20 minutes timestamp
#secret_PRIVATE_KEY = secret_PRIVATE_KEY.encode("UTF-8")
construct_key = '-----BEGIN PRIVATE KEY----- \n' + secret_PRIVATE_KEY + ' \n-----END PRIVATE KEY-----'
formatted_construct_key = construct_key.encode("UTF-8")

header = {
    "alg": "ES256",
    "kid": secret_KEY_ID,
    "typ": "JWT"
}

payload = {
    "iss": secret_ISSUER_ID,
    "exp": EXPIRATION_TIME,
    "aud": "appstoreconnect-v1"
}

# Create the JWT
token = jwt.encode(header, payload, formatted_construct_key)

# API Request
JWT = 'Bearer ' + token.decode()

# Parameters for the API call (daily data extracted from t-1)
frequency = 'DAILY'
reportDate = date.today() - timedelta(days=1)
vendor_id = 000000 # INSERT VENDOR ID HERE

# API call API and header setup
URL = f'https://api.appstoreconnect.apple.com/v1/salesReports?filter[frequency]={frequency}&filter[reportDate]={reportDate}&filter[reportSubType]=SUMMARY&filter[reportType]=SALES&filter[vendorNumber]={vendor_id}'
HEAD = {'Authorization': JWT}

# The actual call
r = requests.get(URL, headers=HEAD)

# Decompress the gzipped output
file_content = gzip.decompress(r.content).decode('utf-8')

# format decompressed output (tsv format to pd dataframe)
def str2frame(estr, sep = '\t', lineterm = '\n', set_header = True):
    dat = [x.split(sep) for x in estr.split(lineterm)][0:-1]
    cdf = pd.DataFrame(dat)
    if set_header:
        cdf = cdf.T.set_index(0, drop = True).T # flip, set ix, flip back
    return cdf
cdf = str2frame(file_content)

# Get todays date to pass into f string csv file name
now = datetime.now()
formatted_now=now.strftime("%d.%m.%Y")
formatted_now_clean = str.replace(formatted_now, '.', '-')

# class that takes object and saves it to blobstorage(kinda overkill to do this in a class)
Azurecontainer_name = "Your azure containername"
folder_path = "yourFolder\subFolder"

class tsvtoblob():
    def __init__(self, element):
        self.element = element

    def writeToBlob(self):
        blob_block = ContainerClient.from_connection_string(
            conn_str= secret_CONN_STR,
            container_name = Azurecontainer_name
            )
        output = io.StringIO()
        partial =self.element
        output = partial.to_csv(encoding='utf-8')  
        name = f"{folder_path}{formatted_now_clean}.csv"
        blob_block.upload_blob(name, output, overwrite=True, encoding='utf-8')

# initialization
tsvtoblob(cdf).writeToBlob()
