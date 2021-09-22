import requests
import pandas as pd
import pathlib
import json
import os

url = os.environ['TICKETS_IP']

response = requests.get(url, verify=False)
data_json = response.json()

df = pd.DataFrame.from_dict(data_json)
df['pic_id'] = df['price'].astype(str) + "_" + df['number'].astype(str)

img_values = df[['pic', 'pic_id']].values.tolist()

for img_value in img_values:
    img_path = '/var/www/html/img/' + 'oh_' + img_value[1] + '.jpg'
    file = pathlib.Path(img_path)

    if not file.exists():
        # print("Downloading image...")
        pull_img= requests.get(img_value[0], stream=True)
        if(pull_img.ok):
            with open(img_path, 'wb+') as file:
                file.write(pull_img.raw.read())
        # print("Finished Download")

