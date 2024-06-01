import json
import sys

# Opening JSON file
f = open('./cover_contracts.json')
  
# returns JSON object as 
# a dictionary
data = json.load(f)
  
# Iterating through the json
# list
print("WITH v1_product_info as (")
for key in data:
    print("SELECT \'{}\' as contract_address, \'{}\' as product_name,  \'{}\' as product_type, \'{}\' as date_added, 'v1' as syndicate UNION ALL".format(key, data[key]["name"], data[key]["type"], data[key]["dateAdded"]))
print(")")
# Closing file
f.close()
sys.exit()
