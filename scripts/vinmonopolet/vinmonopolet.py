# Get all products from vinmonopolet
import requests
import json
import pprint
import pandas as pd
import pickle
import time

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Content-Type": "application/json"
}
wines_pr_page = 1000
page = 0
try:
    with open("data/polet.json", 'r') as f:
        pol_data_json = json.load(f)
    f.close()
except:
    print("No such file available, generate json.")
    pol_data_json = {}

url = f"https://www.vinmonopolet.no/api/search?q=:relevance&searchType=product&currentPage={page}&fields=FULL&pageSize={wines_pr_page}"
# https://www.vinmonopolet.no/search/?q=:relevance&searchType=product&currentPage=2
print(f"Processing page nr {page} with url: {url}.")

r = requests.get(url, headers=headers)
page_json = json.loads(r.text)
page_product_list = page_json['productSearchResult']['products']
page = page_json['productSearchResult']['pagination']['currentPage']
pages = page_json['productSearchResult']['pagination']['totalPages']

for page_nr in range(0, pages+1):

    url = f"https://www.vinmonopolet.no/api/search?q=:relevance&searchType=product&currentPage={page_nr}&fields=FULL&pageSize={wines_pr_page}"
    print(f"Processing page nr {page_nr}/{pages}.")

    # Sleep for a second before moving on to next page such that the requests are not exceeded.
    time.sleep(1)

    r = requests.get(url, headers=headers)
    page_json = json.loads(r.text)
    page_product_list = page_json['productSearchResult']['products']

    for product in page_product_list:
        # pprint.pprint(product)

        # product_url = 'https://www.vinmonopolet.no' + product['url']
        volume = product['volume']['value']
        name = product['name'] + " " + str(volume) + "L" + " " + product['main_category']['name'] #Maybe need to add the category since some people are just useless.. using the same name for all different wines..
        

        # # This should be done "offline" and not when getting information
        # instance = [
        #         name,
        #         product['main_country']['name'] if product["main_country"] is not None else "-", #country
        #         product['district']['name'] if product["district"] is not False else "-",
        #         product['main_category'] if product["main_category"] is not None else "-",
        #         product['main_sub_category'] if product["main_sub_category"] is not None else "-",
        #         product['price']['value'] if product["price"] is not None else "-",
        #         product['volume']['value'] if product["volume"] is not None else "-",
        #         product['litrePrice']['value'] if product["litrePrice"] is not None else "-",
        #         product['expired'] if product["expired"] is not None else "-", #True/false if it is still in inventory
        #         product['buyable'] if product["buyable"] is not None else "-",
        #         product_url,
        # ]
        # pol_data_list.append(instance)
        availability_text = product['availability']['storeAvailability']['mainText']
        if availability_text is not 'Utsolgt i alle butikker - kan ikke bestilles': 
            if name in pol_data_json:
                # print(f"key already in json: {name}")
                # pprint.pprint(product)
                # pprint.pprint(pol_data_json[name])
                # pol_dups[name] = product
                
                # TODO: Need to fix this shit with multiple entries of the same goddamn wine. Could be nice to find bargains at Polet!
                # https://www.vinmonopolet.no/search?q=Ch.%20L%C3%A9oville%20Poyferr%C3%A9%202018:relevance&searchType=product
                # Maybe add extra layer for checking availability compared to price, so could have
                # {best_online_price: X, best_local_price: Y, price_diff: Z}

                
                if availability_text == 'Kan bestilles til alle butikker':
                    if product['price']['value'] < pol_data_json[name]['price']['value']:
                        pol_data_json[name]['oldPrice']['value'] = pol_data_json[name]['price']['value']
                        pol_data_json[name]['price']['value'] = product['price']['value']

                        pol_data_json[name]['oldLitrePrice']['value'] = pol_data_json[name]['litrePrice']['value']
                        pol_data_json[name]['litrePrice']['value'] = product['litrePrice']['value']
                        
                        pol_data_json[name]['url'] = product['url']
                        pol_data_json[name]['expired'] = product['expired']
                        pol_data_json[name]['buyable'] = product['buyable']
                        pol_data_json[name]['availability'] = product['availability']
                        print(f"Product: {name} has changed price from {pol_data_json[name]['oldPrice']['value']} to {pol_data_json[name]['price']['value']}, someone call da police!")

                elif availability_text == 'Kan kjøpes i butikk med varen på lager':
                    #If this could be a local bargain, add it to the possible bargain list
                    pol_data_json[name]['localBargains'].append({'price': pol_data_json[name]['price']['value'],
                    'litrePrice': pol_data_json[name]['litrePrice']['value'],
                    'local_url': product['url'] })
            
            else:
                product['oldPrice'] = {'value' : product['price']['value']}
                product['oldLitrePrice'] = {'value': product['litrePrice']['value']}
                
                # Add a list to fill with possible local bargains.
                product['localBargains'] = []
                if availability_text == 'Kan kjøpes i butikk med varen på lager':
                    product['localBargains'].append({'price': product['price']['value'],
                    'litrePrice': product['litrePrice']['value'],
                    'local_url': product['url'] })
                pol_data_json[name] = product

        else:
            print('Discard entry due to NO availability..') 


# Temporary store the data
with open("data/polet.json", 'w') as f:
    json.dump(pol_data_json, f)
f.close()
