import requests
import json
import time

accountName = "alviclpoc"
environment = "vtexcommercestable"

url_create_collection = f"https://{accountName}.{environment}.com.br/api/catalog/pvt/collection/"

X_VTEX_API_AppKey = "vtexappkey-alviclpoc-MAZKJF"
X_VTEX_API_AppToken = "SKOXLKIJAKTVYUFTOINJIDSVGUCIHPSEDJWMEUBTTNGUUBKFPHOKKYNLUVSTRKPHBWGRMKNXZZQDADPPPNMZXSIABZAUDSPAINEQALODHGQHQXAVJCNKJLTYGZCIXCNA"

products = [{'id': 1, 'name': 'hola'}, {'id': 2, 'name': 'adios'}]

headers = {
    'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
    'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Connection': 'keep-alive',
    }

headers_multiform = {
    'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
    'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
    'Accept': 'application/json',
    'Connection': 'keep-alive',
    }

csv_string = 'SKU,PRODUCT,SKUREFID,PRODUCTREFID\n'
for product in products:
    csv_string += f'{product["id"]},,,\n'
print(csv_string)
payload_create_collection = {
  "Name": "TEST_2023_03_22_F",
  "Description": "AEIOU",
  "Searchable": False,
  "Highlight": False,
  "DateFrom": "2023-03-21T00:00:00-03:00",
  "DateTo": "2023-04-12T23:59:00-03:00"
}
response = requests.request("POST", url_create_collection, headers=headers, json=payload_create_collection)
res = json.loads(response.text)
print(response.text)
print(f"THE ID OF THE COLLECTION TEST_2023_03_22 IS {res['Id']}")
url_load_collection = f"https://{accountName}.{environment}.com.br/api/catalog/pvt/collection/{res['Id']}/stockkeepingunit/importinsert"
csv_file = {'file': ('collection_products.csv', csv_string, 'text/csv')}
time.sleep(20)
response_load_collection = requests.request("POST", url_load_collection, headers=headers_multiform, files=csv_file)
print(response_load_collection.text)
response_load_collection = requests.request("POST", url_load_collection, headers=headers_multiform, files=csv_file)
print(response_load_collection.text)


