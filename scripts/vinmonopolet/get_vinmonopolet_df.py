# Get all products from vinmonopolet
import requests
import json
import pprint
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import pickle
import time
import re

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Content-Type": "application/json"
}
wines_pr_page = 1000
page = 0

#### Load DF containing POLDATA
column_names = ["ExtendedName","Name", "ProductID" ,"Country", "District" ,"Category", "SubCategory","Year", "Price", "OldPrice", "Volume", "LitrePrice", "OldLitrePrice", "Expired", "Buyable","BestLocalPrice","BestLocalLitrePrice","PossibleBargain","BestLocalURL", "URL"]
try:
    pol_df = pd.read_pickle("data/polet.pickle") # This df will be used to search for matched indexes of the different entries. For manipulations of entries, the dict will be used with found locations.
except:
    pol_df = pd.DataFrame(columns=column_names)
# print(pol_df.head(10))
df_size = len(pol_df.index)
pol_dict = pol_df.to_dict() #This dict will be used for adding and manipulating entries since working with a dict is way faster than concatinating df's 

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

        #Check if product is already in dataframe
        volume = product['volume']['value']
        extended_name = product['name'] + " L:" + str(volume) + " " + product['main_category']['name']
        product_id = product['code']
        year = [int(s) for s in re.findall(r'\b\d+\b', product['name'])]
        if (len(year) == 0):
            year = [0]
        product_url = 'https://www.vinmonopolet.no' + product['url']
        
        # Try to find a matchin entry
        found = False
        try: 
            match_index = pol_df[pol_df['ExtendedName'] == extended_name].index.values.astype(int)[0] # These entries should always be of size 1 or zero
            print(match_index)
            found = True
        except:
            # print("No matches found. Add new entry!")
            found = False
        

        # Check availability
        availability_text = product['availability']['storeAvailability']['mainText']
        
        
        if (found == False):
            # No match found.. Insert this instance into df (dict of df). This will take a while the first time around, but that does not really matter!
            pol_dict["ExtendedName"][df_size] = extended_name
            pol_dict["Name"][df_size] = product['name']
            pol_dict["ProductID"][df_size] = product_id
            pol_dict["Country"][df_size] = product['main_country']['name'] if bool(product["main_country"]) is not False else "-"
            pol_dict["District"][df_size] = product['district']['name'] if bool(product["district"]) is not False else "-"
            pol_dict["Category"][df_size] = product['main_category']['name'] if bool(product["main_category"]) is not False else "-"
            pol_dict["SubCategory"][df_size] = product['main_sub_category'] if bool(product["main_sub_category"]) is not False else "-"
            pol_dict["Year"][df_size] = year[-1] #Get the last entry of the years list, which should correspond to the year that the name is supposed to end with.
            pol_dict["Price"][df_size] = product['price']['value'] if bool(product["price"]) is not False else "-"
            pol_dict["OldPrice"][df_size] = product['price']['value'] if bool(product["price"]) is not False else "-"
            pol_dict["Volume"][df_size] = product['volume']['value'] if bool(product["volume"]) is not False else "-"
            pol_dict["LitrePrice"][df_size] = product['litrePrice']['value'] if bool(product["litrePrice"]) is not False else "-"
            pol_dict["OldLitrePrice"][df_size] = product['litrePrice']['value'] if bool(product["litrePrice"]) is not False else "-"
            pol_dict["Expired"][df_size] = product['expired'] if bool(product["expired"]) is not False else "-"
            pol_dict["Buyable"][df_size] = product['buyable'] if bool(product["buyable"]) is not False else "-"
            pol_dict["BestLocalPrice"][df_size] = product['price']['value'] if bool(product["price"]) is not False else "-"
            pol_dict["BestLocalLitrePrice"][df_size] = product['litrePrice']['value'] if bool(product["litrePrice"]) is not False else "-"
            pol_dict["PossibleBargain"][df_size] = product['buyable'] if bool(product["buyable"]) is not False else "-"
            pol_dict["BestLocalURL"][df_size] = product_url
            pol_dict["URL"][df_size] = product_url
            df_size += 1
            
        else:
            # If match is found in df, then update the values based on availability

            # if availability_text is not 'Utsolgt i alle butikker - kan ikke bestilles': 
            # Worth it to have this as a doublke check on expiration and or buyable? Should be the same kind off.

            if availability_text == 'Kan bestilles til alle butikker':
                if product['price']['value'] < pol_dict["Price"][match_index]:
                    # If new price found is lower than what was previously, then update instance with new prices
                    pol_dict["OldPrice"][match_index] = pol_dict["Price"][match_index]
                    pol_dict["Price"][match_index] = product['price']['value']

                    pol_dict["OldLitrePrice"][match_index] = pol_dict["LitrePrice"][match_index]
                    pol_dict["LitrePrice"][match_index] = product['litrePrice']['value']

                    pol_dict["URL"][match_index] = product_url
                    pol_dict["ProductID"][match_index] = product_id
                    pol_dict["Expired"][match_index] = product['expired'] if bool(product["expired"]) is not False else "-"
                    pol_dict["Buyable"][match_index] = product['buyable'] if bool(product["buyable"]) is not False else "-"

                    # If the one that can be ordered has a lower price than the local one, then update this price too.
                    if product['price']['value'] < pol_dict['BestLocalPrice'][match_index]:
                        pol_dict["BestLocalPrice"][match_index] = product['price']['value'] 
                        pol_dict["PossibleBargain"][match_index] = product['buyable']
                        pol_dict["BestLocalLitrePrice"][match_index] = product['litrePrice']['value'] 
                        pol_dict["BestLocalURL"][match_index] = product_url
            
                    print(f"Product: {pol_dict['Name'][match_index]} has changed price from {pol_dict['OldPrice'][match_index]} to {pol_dict['Price'][match_index]}, someone call da police!")                    
                
                elif pol_dict["ProductID"][match_index] == product_id:
                    # If the product has the same product ID, but changed price, then update the price too!
                    pol_dict["OldPrice"][match_index] = pol_dict["Price"][match_index]
                    pol_dict["Price"][match_index] = product['price']['value']

                    pol_dict["OldLitrePrice"][match_index] = pol_dict["LitrePrice"][match_index]
                    pol_dict["LitrePrice"][match_index] = product['litrePrice']['value']

            elif availability_text == 'Kan kjøpes i butikk med varen på lager':
                #If this could be a local bargain, add best local bargain price if it is lower, else, it is just an overpriced bottle so fuck off
                if product['price']['value'] < pol_dict['BestLocalPrice'][match_index]:
                    pol_dict["BestLocalPrice"][match_index] = product['price']['value'] 
                    pol_dict["PossibleBargain"][match_index] = product['buyable']
                    pol_dict["BestLocalLitrePrice"][match_index] = product['litrePrice']['value'] 
                    pol_dict["BestLocalURL"][match_index] = product_url
            
            else:
                # If product is NOT available anymore, is it worth keeping track of it? It will still be in the dict
                print(f"This product is NOT available so not worth adding it if an available product is already in the df.")

                # TODO: Need to fix this shit with multiple entries of the same goddamn wine. Could be nice to find bargains at Polet!
                # https://www.vinmonopolet.no/search?q=Ch.%20L%C3%A9oville%20Poyferr%C3%A9%202018:relevance&searchType=product
                # Maybe add extra layer for checking availability compared to price, so could have
                # {best_online_price: X, best_local_price: Y, price_diff: Z}
                # IS this really fixed now?? :P Todo; Test! 

# Store pandas dataframe.
pol_df = pd.DataFrame(pol_dict)
pol_df.to_pickle("data/polet.pickle")
