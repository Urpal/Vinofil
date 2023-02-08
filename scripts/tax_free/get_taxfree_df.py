# from algoliasearch.search_client import SearchClient
import requests
import json
from pprint import pprint
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import pickle
import time

# Use a mapping variable or something when merging the datasets. 
# Hvitvin, Rødvin, Sterkvin, Brennevin, Musserende vin, Rosévin, Sake, Perlende vin, Aromatisert vin, 

url = "https://namx6ho175-dsn.algolia.net/1/indexes/*/queries"
hits_per_page = 24

params = {
    "x-algolia-agent":"Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20JS%20Helper%20(3.10.0)",
    "x-algolia-api-key": "55252987cc07b733b24f13fc4754f42e",
    "x-algolia-application-id": "NAMX6HO175",
}

payload = {
    "requests": [ 
        # {"indexName":"prod_products",
        # "params":'facetFilters=[["categoriesLevel0.no:Alkohol"]&facets=["inStockInCodes","inStockInCodes.no","onlineExclusive","onlineExclusive.no","norwegian","norwegian.no","price.NOK","alcoholByVolume","alcoholByVolume.no","salesAmount","salesAmount.no","year.no","year.no.no","country.no","country.no.no","wineCultivationArea.no","wineCultivationArea.no.no","wineGrowingAhreaDetail.no","wineGrowingAhreaDetail.no.no","organic.no","organic.no.no","suggarContent.no","suggarContent.no.no","wineGrapes.no","wineGrapes.no.no","colour.no","colour.no.no","sweetness.no","sweetness.no.no","tasteFill.no","tasteFill.no.no","tasteIntensity.no","tasteIntensity.no.no","tasteTheAcid.no","tasteTheAcid.no.no","brandName.no","brandName.no.no","WhiskyRegion.no","WhiskyRegion.no.no","glutenFree.no","glutenFree.no.no","Packaging.no","Packaging.no.no","categoriesLevel0.no","categoriesLevel1.no"]&filters= availableInAirportCodes:OSL AND allCategories:941&hitsPerPage=24&page=1&&tagFilters=&query='}, # Unknown parameter: &tagFilters:  hmm? Found it in the inspect.., also; &query: is unknown?
        {"indexName":"prod_products",
        "params":f'analytics=false&clickAnalytics=false&facets=["categoriesLevel0.no"]&filters= availableInAirportCodes:OSL AND allCategories:941&hitsPerPage={hits_per_page}&page=0&query= '}
        ]} 
        # This could possibly be refined in order to get only red wines and so on, but that does not make sense.
        # Trying to check all alcoholic beverages available vs Polet and maybe vivino ratings ;) 


#### Load DF containing POLDATA
column_names = ["Name", "NameExtended", "Country", "Area", "Category", "Year", "Brand",  "PriceNOK","PriceNOKOld", "LitrePriceNOK", "LitrePriceNOKOld", "PriceEUR", "PriceEUROld", "LitrePriceEUR", "LitrePriceEUROld", "Unit", "Amount", "Description", "Percentage", "Grapes", "URL"]
try:
    tf_df = pd.read_pickle("data/tf.pickle") # This df will be used to search for matched indexes of the different entries. For manipulations of entries, the dict will be used with found locations.
except:
    tf_df = pd.DataFrame(columns=column_names)
# print(tf_df.head(10))
df_size = len(tf_df.index)
tf_dict = tf_df.to_dict() #This dict will be used for adding and manipulating entries since working with a dict is way faster than concatinating df's 


# Get initial response from JS API call just to find number of pages needed to be looped
r = requests.post(url, params=params, json=payload)
json_info = r.json()

pages = json_info['results'][0]['nbPages'] #This is not necessarily "all products / products per page", after manual checks, even though it is described as 1404 alcohol products, there is actually only 909 lol :P
# # pages = 2
# data_list = []
# tax_free_data_json = {}

for page_nr in range(1, pages+1):
    time.sleep(1) # sleep for 500 ms such that I do not call the API too much.
    print(f"Processing page nr {page_nr}/{pages}.")
    #For each page number, update the payload to get next page.
    payload = {
    "requests": [ 
        # {"indexName":"prod_products",
        # "params":'facetFilters=[["categoriesLevel0.no:Alkohol"]&facets=["inStockInCodes","inStockInCodes.no","onlineExclusive","onlineExclusive.no","norwegian","norwegian.no","price.NOK","alcoholByVolume","alcoholByVolume.no","salesAmount","salesAmount.no","year.no","year.no.no","country.no","country.no.no","wineCultivationArea.no","wineCultivationArea.no.no","wineGrowingAhreaDetail.no","wineGrowingAhreaDetail.no.no","organic.no","organic.no.no","suggarContent.no","suggarContent.no.no","wineGrapes.no","wineGrapes.no.no","colour.no","colour.no.no","sweetness.no","sweetness.no.no","tasteFill.no","tasteFill.no.no","tasteIntensity.no","tasteIntensity.no.no","tasteTheAcid.no","tasteTheAcid.no.no","brandName.no","brandName.no.no","WhiskyRegion.no","WhiskyRegion.no.no","glutenFree.no","glutenFree.no.no","Packaging.no","Packaging.no.no","categoriesLevel0.no","categoriesLevel1.no"]&filters= availableInAirportCodes:OSL AND allCategories:941&hitsPerPage=24&page=1&&tagFilters=&query='}, # Unknown parameter: &tagFilters:  hmm? Found it in the inspect.., also; &query: is unknown?
        {"indexName":"prod_products",
        "params":f'analytics=false&clickAnalytics=false&facets=["categoriesLevel0.no"]&filters= availableInAirportCodes:OSL AND allCategories:941&hitsPerPage={hits_per_page}&page={page_nr}&query= '}
        ]} 
    # Get json info from every page.
    r = requests.post(url, params=params, json=payload)
    json_info = r.json()    

    for hit in json_info['results'][0]['hits']:
        # pprint(hit)
        product_url = "https://www.tax-free.no/no" + hit['url']
        nameExtended = hit['erpName']['en'] # Do we need to have multiple of this one too? 
        try:
            name = hit['name']['en'] 
        except KeyError as e:
            # if e == KeyError:
                name = hit['name']['no']
            # else: 
            #     print("Need another one.")

        category = hit['allCategoriesNames']['en']

        if 'Whitewine' in category:
            category = 'Whitewine'
        elif 'Redwine' in category:
            category = 'Redwine'
        elif 'Beer' in category:
            category = 'Beer'
        elif 'Spirits' in category:
            category = 'Spirits'
        elif 'Sparkling wine' in category:
            category = 'Sparkling wine'
        else:
            category = 'Other'


        if 'en' in hit['unit'].keys():
            unit = hit['unit']['en']
        elif 'no' in hit['unit'].keys():
            unit = hit['unit']['no']


        # Try to find a matching entry in available df, if not add entry
        found = False
        try: 
            match_index = tf_df[tf_df['Name'] == name].index.values.astype(int)[0] # These entries should always be of size 1 or zero
            print(match_index)
            found = True
        except:
            # print("No matches found. Add new entry!")
            found = False

        # If there is not a equal name in the dataframe, add info
        if (found == False):
            # No match found.. Insert this instance into df (dict of df). This will take a while the first time around, but that does not really matter!
            tf_dict["Name"][df_size] = name
            tf_dict["NameExtended"][df_size] = nameExtended
            tf_dict["Country"][df_size] =  hit['country']['en'] if hit["country"] is not None else "-"
            tf_dict["Area"][df_size] =  hit['wineCultivationArea']['en'] if hit["wineCultivationArea"] is not None else "-"
            tf_dict["Category"][df_size] = category
            tf_dict["Year"][df_size] = hit['year']['en'] if hit["year"] is not None else "-"
            tf_dict["Brand"][df_size] = hit['brandName']['no'] if hit["brandName"] is not None else "-"
            tf_dict["PriceNOK"][df_size] = hit['price']['NOK']
            tf_dict["PriceNOKOld"][df_size] = hit['price']['NOK']
            tf_dict["LitrePriceNOK"][df_size] = float(hit['price']['NOK'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
            tf_dict["LitrePriceNOKOld"][df_size] = float(hit['price']['NOK'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
            tf_dict["PriceEUR"][df_size] = hit['price']['EUR']
            tf_dict["PriceEUROld"][df_size] = hit['price']['EUR']
            tf_dict["LitrePriceEUR"][df_size] = float(hit['price']['EUR'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
            tf_dict["LitrePriceEUROld"][df_size] = float(hit['price']['EUR'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
            tf_dict["Unit"][df_size] = unit
            tf_dict["Amount"][df_size] = float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
            tf_dict["Description"][df_size] =  hit['description']['en'] if hit["description"] is not None else "-"
            tf_dict["Percentage"][df_size] = hit['alcoholByVolume'] if hit['alcoholByVolume'] is not None else "-"
            tf_dict["Grapes"][df_size] = hit['wineGrapes']['en'] if hit["wineGrapes"] is not None else "-"
            tf_dict["URL"][df_size] = product_url

            df_size += 1
            
            # hit['age'], #age = 
            # hit['wineGrowingAhreaDetail'] if hit["wineGrowingAhreaDetail"] is not None else "-", #area_dets = 
            # hit['categoriesLevel3']['en'], #category_levels = 
            # hit['categoryName']['en'], #category_name = 
            # hit['colour']['en'], #color =     
            # hit['erpName']['en'], #name_w_volume = 
            # hit['suggarContent']['no'], #sugar_content = 
            # hit['sweetness'], #sweetness = 
            # hit['taste'], #taste = 
            # hit['tasteFill'], #fill = 
            # hit['tasteIntensity'], #intensity = 
            # hit['tasteTheAcid'], #acidity = 
            
        else:
            # If the name is already available in the df, check if price has changed.
            # Here should not be multiple instances of the same bottle as with vinmonopolet, therefore, we do not need to change up too much of other stuff,
            if (tf_dict["PriceNOK"][match_index] != hit['price']['NOK'] or tf_dict["PriceEUR"][match_index] != hit['price']['EUR']):
                # If new price found is lower than what was previously, then update instance with new prices
                tf_dict["PriceNOKOld"][match_index] = tf_dict["PriceNOK"][match_index]
                tf_dict["PriceNOK"][match_index] = hit['price']['NOK']

                tf_dict["LitrePriceNOKOld"][match_index] = tf_dict["LitrePriceNOK"][match_index]
                tf_dict["LitrePriceNOK"][match_index] = float(hit['price']['NOK'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
                
                tf_dict["PriceEUROld"][match_index] = tf_dict["PriceEUR"][match_index]
                tf_dict["PriceEUR"][match_index] = hit['price']['EUR']

                tf_dict["LitrePriceEUROld"][match_index] = tf_dict["LitrePriceEUR"][match_index]
                tf_dict["LitrePriceEUR"][match_index] = float(hit['price']['EUR'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0

tf_df = pd.DataFrame(tf_dict)
tf_df.to_pickle("data/tf.pickle")