import json
import pandas as pd
import pprint

#  Open json data
with open("data/polet.json", 'r') as f:
    pol_data_json = json.load(f)
f.close()

pol_data_list = []

for name,product in pol_data_json.items():
    if product['main_category']['name'] is not 'Gaveartikler og tilbehør':
        # pprint.pprint(product)

        product_url = 'https://www.vinmonopolet.no' + product['url']
        name = product['name']
        
        # Todo add localBargains
        # if len(product['localBargains']) is not 0:
        # Local bargains could look like: 


        # This should be done "offline" and not when getting information
        try:
            instance = [
                    name,
                    product['main_country']['name'] if bool(product["main_country"]) is not False else "-", #country
                    product['district']['name'] if bool(product["district"]) is not False else "-",
                    product['main_category']['name'] if bool(product["main_category"]) is not False else "-",
                    product['main_sub_category']['name'] if bool(product["main_sub_category"]) is not False else "-",
                    product['price']['value'] if bool(product["price"]) is not False else "-",
                    product['oldPrice']['value'] if product["oldPrice"] is not None else "-",
                    product['volume']['value'] if bool(product["volume"]) is not False else "-",
                    product['litrePrice']['value'] if bool(product["litrePrice"]) is not False else "-",
                    product['oldLitrePrice']['value'] if bool(product["oldLitrePrice"]) is not False else "-",
                    product['expired'] if bool(product["expired"]) is not False else "-", #True/false if it is still in inventory
                    product['buyable'] if bool(product["buyable"]) is not False else "-",
                    product_url,
            ]
            pol_data_list.append(instance)
        except KeyError:
            pprint.pprint(product)


column_names = ["Name","Country", "District" ,"Category", "SubCategory", "Price", "OldPrice", "Volume", "litrePrice", "oldLitrePrice", "expired", "buyable", "URL"]
df = pd.DataFrame(data=pol_data_list, columns=column_names)
print(df.head(10))

# Store pandas dataframe.
df.to_pickle("data/polet.pickle")
print(len(df.index))

#Test reading the same data
tfdf = pd.read_pickle("data/polet.pickle")
print(tfdf.head(10))
print(len(tfdf.index))