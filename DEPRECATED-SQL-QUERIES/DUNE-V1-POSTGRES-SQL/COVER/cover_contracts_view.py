import json
import sys

# Opening JSON file
f = open('./cover_contracts.json')


# returns JSON object as 
# a dictionary
data = json.load(f)

  
# Iterating through the json
# list
print("CREATE OR REPLACE VIEW dune_user_generated.nexus_v1_product_info_view(date_added, contract_address, name, type, syndicate) AS VALUES")
counter = 1
for key in data:
    counter += 1
    print("(\'{}\'::date, \'{}\'::bytea, \'{}\'::text, \'{}\'::text, '{}'::text ),".format(data[key]["dateAdded"], key, data[key]["name"], data[key]["type"], 'v1'))
# Closing file
f.close()

#print("Added {} entries".format(counter))

sys.exit()


