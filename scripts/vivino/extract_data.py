# Import packages
import requests
import json
import pandas as pd
import time
import pprint
import pickle as pickle

#Load json string list
json_string_list = []
#Read the pickeled data:
with open ('data/Australia_vivino_data.pickle', 'rb') as fp:
    item_list = pickle.load(fp)
print(item_list)

# Create New Dataframe to store output
column_names=["Wine", "Rating", "Ratings count", "Price", "Currency" , "Bottle type" ,"Size",
"Country", "Region", "Winery", "Wine type", "Variety", "Wine name short", "Wine name short w year",
"Year", "Wine ID vivino", "Vintage type", "Is natural", "Acidity1", "Acidity2", "Acidity desc",
"Body", "Body desc", "fizziness", "intensity", "sweetness", "tannin", "user struct count", "cacl struct count" ] #,"grapes"]
df = pd.DataFrame(columns = column_names)


approved_country_code_list = [] # This is the ones from land_list that actually returns some values..
print("Looping through params and running requests. Might take some time...")
results = []
for json_string in json_string_list:
    try: #THIS is NOT thee way to go.. Try rather to just fill in everything directly as done before? If not, fin a way around the None shit.
        instance = [
            json_string['vintage']['name'],       # Full name
            json_string["vintage"]["statistics"]["ratings_average"],  # Rating average
            json_string["vintage"]["statistics"]["ratings_count"],    # Ratings count
            round(json_string["price"]["amount"], 2) if json_string["price"] is not None else "-", #Price
            json_string["price"]["currency"]['code'],     # Currency
            json_string["price"]["bottle_type"]['short_name'],    # Bottle type
            json_string["price"]["bottle_type"]['volume_ml'],     # Bottle size
            json_string['vintage']['wine']['region']['country']['name'], # Country name
            json_string['vintage']['wine']['region']['name'],             # Region name
            json_string["vintage"]["wine"]["winery"]["name"],             # Winery name
            json_string['vintage']['wine']['type_id'],                    # Wine type ID
            json_string['vintage']['wine']['style']['varietal_name'] if json_string['vintage']['wine']['style'] is not None else "-", # Variety name, i.e. grape name?
            json_string["vintage"]["wine"]["name"],                       # Wine name short
            f'{json_string["vintage"]["wine"]["name"]} {json_string["vintage"]["year"]}', # Wine name short with year
            json_string["vintage"]["year"],                               # Year
            json_string["vintage"]["wine"]["id"],                         # Vivino wine ID
            json_string['vintage']['wine']['vintage_type'],               # Vintage type; TODO: Er det årgangsvin? 
            json_string['vintage']['wine']['is_natural'],                 # Bool: Is natural?
            json_string['vintage']['wine']['taste']['structure']['acidity'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",      # Tastes
            json_string['vintage']['wine']['style']['acidity'] if json_string['vintage']['wine']['style'] is not None else "-",             
            json_string['vintage']['wine']['style']['acidity_description'] if json_string['vintage']['wine']['style'] is not None else "-",
            json_string['vintage']['wine']['style']['body'] if json_string['vintage']['wine']['style'] is not None else "-", 
            json_string['vintage']['wine']['style']['body_description'] if json_string['vintage']['wine']['style'] is not None else "-", 
            json_string['vintage']['wine']['taste']['structure']['fizziness'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",
            json_string['vintage']['wine']['taste']['structure']['intensity'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",
            json_string['vintage']['wine']['taste']['structure']['sweetness'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",
            json_string['vintage']['wine']['taste']['structure']['tannin'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",
            json_string['vintage']['wine']['taste']['structure']['user_structure_count'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-",
            json_string['vintage']['wine']['taste']['structure']['calculated_structure_count'] if json_string['vintage']['wine']['taste']['structure'] is not None else "-"#,
            # json_string['vintage']['grapes']# Grapes
        ]
        results.append(instance) 

    except Exception as e:
        print(e)
        print("Something wrong with the data...")
        pprint.pprint(json_string)

# Appends the scraped results to the dataframe
df = pd.DataFrame(results, columns=column_names)
print("Dataframe size before remove dups: ")
print(df.size())
#Drop duplicates. Should I also check which land has same dups? 
df.drop_duplicates().reset_index(drop=True) #This does not make sense to have together with the web scraping tbh..
print("Dataframe size after remove dups: ")
print(df.size())

# Store the complete data as csv format
df.to_csv("vivtest.csv", index=False)
