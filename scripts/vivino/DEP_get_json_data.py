# Import packages
import requests
import json
import pandas as pd
import time
import pprint
import pickle

# Takes WAY too long time to do everything in one script if we want to make sure that all wines gets covered by using all land codes.
# Therefore, this json part is used to request all information as json before extract_data is used to remove duplicates and place the data into a proper useful dataframe


# 1. Retrieve list of all country codes in the world (yes, why not :P)
# Useless with dataframe when list is desired for iterating.
# df = pd.read_csv("data/country_codes_alpha_2_list.csv")
# print(df.head(10))
import csv
with open('data/country_codes_alpha_2_list_short.csv', newline='\n') as f: #remove test when done
    reader = csv.reader(f)
    land_dict = {rows[0]: rows[1] for rows in reader}
    # land_list = list(reader)
    # land_dict = csv.DictReader(reader, fieldnames=['country', 'country_code'])
    print(land_dict)

# print(data)
# for land in data: 
#   #land[1] is the alpha 2 country code whereas land [0] is the actual country name
    

#### Real gathering of the data ####
#Create Headers for the browser done for the searches
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
}
wines_pr_page = 50 #max 50!


# 2. modularize it such that the data retrieval is done for specified "markets" or countries for all country codes. 
paramlist = [] #Parameter list of dictionaries to be used in the searches.
for land in land_list:
    paramlist.append(
        # Create Parameters
        {
            "country_code": land[1], #NB! This one seems important for "available" market that is searched for..
            "grape_filter": "varietal",
            "min_rating": 1,
            "order_by": "price",
            "order": "desc",
            "per_page": wines_pr_page, #Maybe use 100 instead to not use thaaat many requests? :P This takes forever..
            "page": 1,
            "price_range_max": 5000,
            "price_range_min": 1,
            # Filters
            "country_codes[]": ["es", "fr", "it", "de", "au", "us", "pt", "ar", "cl", "pt", "at", "cn", "za", "ro", "hu", "md", "gr", "nz", "bg", "ch"],  # "FR", "IT", "DE", "CL", "PT", "AU", "AT", "AR", "US" <-- can add more country codes here
            "wine_type_ids[]": ["1" , "2", "3", "4"],
            "currency_code": "USD"}
    )

json_string_list = []
approved_country_code_list = [] # This is the ones from land_list that actually returns some values..
print("Looping through params and running requests. Might take some time...")
for params in paramlist:
    # Performs inital request for count of IDs
    # Does not seem that this api explore actually can return all wines? 
    #Problem with country/market being set to no, whatever that means..
    r = requests.get('https://www.vivino.com/api/explore/explore?', #?, why should I remove questionmark
                    params=params, headers=headers)
    jsonFile = r.json()
    print(r.json()['explore_vintage']['market'])
    print(r.json()['explore_vintage']['records_matched'])
    # print(r.json()["explore_vintage"]["matches"])
    # print(r.json()['selected_filters'])
    

    # Save data as json
    # with open('data'+params["country_code"]+".json", 'w') as f:
    #     json.dump(jsonFile, f)

    n_matches = r.json()['explore_vintage']['records_matched']
    # Try printing the objects pretty..
    # for match in r.json()["explore_vintage"]["matches"]:
    #     pprint.pprint(match)
    # for i in range(n_matches):
    #     pprint.pprint(r.json()['explore_vintage']['records_matched'][i])


    if n_matches <= 1:
        print("This country code: {} is useless with mathes: {} ".format(params["country_code"], n_matches))
        print()
    else:
        approved_country_code_list.append(land)

    # Pagination
    pages = int(n_matches / wines_pr_page)
    if pages == 0 and n_matches > 0:
        pages = 1

    for i in range(pages):
        # Adds the page to params
        params['page'] = i + 1

        print(f'Requesting data from page: {params["page"]} of {pages}')

        # Performs the request
        attempts = 0 
        response_ok = False
        while attempts < 3 and response_ok == False:
            try:
                r = requests.get('https://www.vivino.com/api/explore/explore?',
                        params=params, headers=headers) # These requestss takes forever... like half a second each.. 
                if r.ok:
                    response_ok = True
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                # raise SystemExit(e)
                attempts += 1
                print("Request exception! Try again since attempts = {attempts}")
                time.sleep(0.5)
                if (attempts == 3):
                    raise SystemExit(e)
        
        
        # 26 946 wines => 1077 pages of 25 wines = 500 seconds isch.
        # This happens for all of the country codes with a lot of duplicate wines unfortunately..
        # AT and AU for instance...
        # BE = Belgium = 35946, is that "available" wines in that country?
        for t in r.json()["explore_vintage"]["matches"]: 
            json_string_list.append(t)

        # Save data aafter each request..
        with open('data/json_wine_data.pickle', 'wb') as fp:
            pickle.dump(json_string_list, fp)
        
print(approved_country_code_list)


# Save data to a pickle file




with open('approved_country_list', 'wb') as country_list_file:
    pickle.dump(approved_country_code_list, country_list_file)



# 3. Merge the dataframes and remove duplicates (there should be a lot duplicates...)
# 4. Save all as csv, pickle or whatever.
# 5. Check number of i.e. spanish wines vs what is available at the Vivino webpage
# 6. Add python file to get data through vinmonopolet API
# 7. Same as above but for Taxfree (might need to webscrape? )

