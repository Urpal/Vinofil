# Import packages
import requests
import pandas as pd
import pprint
import pickle
import csv
import time
import json

## Note 
# This just not really work at all. Gettting a LOT of duplicates, and why do I even care about all of these wines if they are never going to be availableeither way
# Better then to narrow down the search for only wines that are actually available on the tax free.

# def get_vivino(country_list):
#     """
#     This function takes a list of country names as input and returns a json with all wines from the specified countries above 3.5 Stars rating.
#     Args:
#         country_list (List[int]): A list of country names
#     Returns:
#         json: all products
#     """

### TEMP
tf_df = pd.read_pickle("data/tf.pickle")
tf_df = tf_df[~(tf_df['Country'] == '-')] # Remove all entries that do not have a country.

unique_countries = tf_df['Country'].value_counts().to_dict()
# print(unique_countries)
country_list = list(unique_countries.keys())
unique_categories = tf_df['Category'].value_counts().to_dict()
# print(unique_categories)
###

# Get known country codes.
with open('data/country_codes_alpha_2_list.csv', newline='\n') as f:
    reader = csv.reader(f)
    land_dict = {rows[0]: rows[1] for rows in reader}
    # print(land_dict)

#Create Headers for the browser done for the searches
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
}
wines_pr_page = 25 #max 50!
# paramlist = [] #Parameter list of dictionaries to be used in the searches.

# Map country list to country codes
# country_code_matches = []
country_start_time = time.time()
for country in country_list:
    # Initialize own list for each country
    json_string_list = []
    json_string_dict = {}
    if country in land_dict.keys():
        print(f"Country: {country} and code: {land_dict[country]}")
        

        #### Real gathering of the data ####
        # 2. modularize it such that the data retrieval is done for specified "markets" or countries for all country codes. 
        params = {
                "country_code": "NO", #land_dict[country], #NB! This one seems important for "available" market that is searched for..
                "grape_filter": "varietal",
                "min_rating": 3.5,
                "order_by": "price",
                "order": "desc",
                "per_page": wines_pr_page, #Maybe use 100 instead to not use thaaat many requests? :P This takes forever..
                "page": 1,
                "price_range_max": 2500,
                "price_range_min": 1,
                # Filters
                "country_codes[]": [land_dict[country].lower()], #["es", "fr", "it", "de", "au", "us", "pt", "ar", "cl", "pt", "at", "cn", "za", "ro", "hu", "md", "gr", "nz", "bg", "ch"],  # "FR", "IT", "DE", "CL", "PT", "AU", "AT", "AR", "US" <-- can add more country codes here
                "wine_type_ids[]": ["1" , "2", "3", "4", "7"],
                "currency_code": "NOK"}
        

        # Performs inital request for count of IDs
        time.sleep(10) #Sleep for 10 seconds since that is the minimum time vivino has set :P
        r = requests.get('https://www.vivino.com/api/explore/explore?', params=params, headers=headers)
        json_reply = r.json()
        # print(r.json()['explore_vintage']['market'])
        n_matches = r.json()['explore_vintage']['records_matched']
        print(f"Matches: {n_matches}")

        if n_matches <= 1:
            print("This country code: {} is useless with mathes: {} ".format(params["country_code"], n_matches))
            continue 
        else:

            # Pagination of whatever entries might fit.
            pages = int(n_matches / wines_pr_page)
            if pages == 0 and n_matches > 0:
                pages = 1

            for i in range(pages):
                time.sleep(10) #Sleep for 10 seconds since that is the minimum time vivino has set :P
                # Adds the page to params
                params['page'] = i + 1
                print(f'Requesting data from page: {params["page"]} of {pages}')
                start_time = time.time()

                # Performs the request
                attempts = 0 
                response_ok = False
                while attempts < 3 and response_ok == False:
                    try:
                        r = requests.get('https://www.vivino.com/api/explore/explore?',
                                params=params, headers=headers) # These requestss takes forever... like half a second each.. 
                        if r.ok:
                            response_ok = True
                        if r.status_code is not 200:
                            print(f"Status code NOT 200, OK but rather {r.status_code}. This is usually a problem.")
                    except requests.exceptions.RequestException as e:  # This is the correct syntax
                        # raise SystemExit(e)
                        attempts += 1
                        print("Request exception! Try again since attempts = {attempts}")
                        time.sleep(5)
                        if (attempts == 3):
                            # If requests break. Write to screen and store whatever was left found for the country!
                            print("Third exception. The scraper has died.. \n####\nRIP\n####\n")
                            with open('data/{country}_vivino_data.pickle', 'wb') as fp:
                                    pickle.dump(json_string_list, fp)
                            raise SystemExit(e) #Maybe do a continue here instead? Or break this country?
                                
                request_time = time.time()
                matches = r.json()["explore_vintage"]["matches"]
                for match in matches: 
                    # print(f"\nNew match: \n{match}")
                    json_string_list.append(match)
                    if match['vintage']['name'] not in json_string_dict:
                        json_string_dict[match['vintage']['name']] = match
                    else:
                        print(f"Duplicate entry for: {match['vintage']['name']}")
                        print(json_string_dict[match['vintage']['name']])
                handling_time = time.time()

    # Save data after each request for the specified country that is being queried
    with open(f'data/{country}_vivino_data.pickle', 'wb') as fp:
        pickle.dump(json_string_list, fp)
    with open(f'data/{country}_vivino_data.json', 'w') as fjson:
        json.dump(json_string_dict, fjson)

        # storage_time = time.time()
                # print(f"Request time: {(request_time-start_time):.2f}, Handling Time: {(handling_time-start_time):.2f} Storage time: {(storage_time-start_time):.2f}\n")
    country_end_time = time.time()
    print(f"Country scrape time: {(country_end_time-country_start_time):.2f} seconds for {n_matches} matches and {pages} pages")


# Does it make sense to scrape all of this?? :P better way would be to only scrape the ones that fit more strict criterias?      

# 3. Merge the dataframes and remove duplicates (there should be a lot duplicates...)
# 4. Save all as csv, pickle or whatever.
# 5. Check number of i.e. spanish wines vs what is available at the Vivino webpage

