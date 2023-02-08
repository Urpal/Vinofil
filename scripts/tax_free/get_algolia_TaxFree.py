# from algoliasearch.search_client import SearchClient
import requests
import json
from pprint import pprint
import pandas as pd
import pickle
import time

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


# Get initial response from JS API call just to find number of pages needed to be looped
r = requests.post(url, params=params, json=payload)
json_info = r.json()

pages = json_info['results'][0]['nbPages'] #This is not necessarily "all products / products per page", after manual checks, even though it is described as 1404 alcohol products, there is actually only 909 lol :P
# pages = 2
data_list = []
tax_free_data_json = {}

for page_nr in range(1, pages+1):
    time.sleep(0.5) # sleep for 500 ms such that I do not call the API too much.
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
        name = hit['erpName']['en']

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
        

        instance = [
                name,
                hit['description']['en'] if hit["description"] is not None else "-", #description = 
                category,
                hit['year']['en'] if hit["year"] is not None else "-", #year = 
                hit['alcoholByVolume'], #percent = 
                # hit['unit']['en'] if hit["unit"] is not None else "-", #l typically. #unit = 
                unit,
                hit['brandName']['no'] if hit["brandName"] is not None else "-", #brand_name = 
                hit['country']['en'] if hit["country"] is not None else "-", #country = 
                hit['price']['NOK'], #price_NOK =
                hit['price']['EUR'], #price_EUR = 
                hit['salesAmount'],  #liter = 
                float(hit['price']['NOK'])/float(hit['salesAmount']) if hit['salesAmount'] is not None else 0, # NOK per liter
                # hit['availableInAirportCodes'], #airports_availability_list = 
                hit['wineCultivationArea']['en'] if hit["wineCultivationArea"] is not None else "-", #area = 
                hit['wineGrapes']['en'] if hit["wineGrapes"] is not None else "-", #grapes = 
                product_url, 
            
            # hit['age'], #age = 
            # hit['wineGrowingAhreaDetail'] if hit["wineGrowingAhreaDetail"] is not None else "-", #area_dets = 
            # 
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

            # # Anything else needed? Whiskey and so on? 
            # hit['beerType'] #beer_type = 
        ]
        data_list.append(instance)
        tax_free_data_json[name] = hit #Better to use this and make handle script..


column_names = ["Name","Description", "Categories", "Year", "Percentage", "Unit", "Brand", "Country", "Price_NOK", "Price_EUR", "Amount",  "NOK_pr_l", "Area", "Grapes", "URL"]
df = pd.DataFrame(data=data_list, columns=column_names)
print(df.head(10))

# Store pandas dataframe.
df.to_pickle("data/taxFree.pickle")
print(len(df.index))


#Test reading the same data
tfdf = pd.read_pickle("data/taxFree.pickle")
print(tfdf.head(10))
print(len(tfdf.index))

# Temporary store the data
with open("data/tax_free.json", 'w') as f:
    json.dump(tax_free_data_json, f)
f.close()
