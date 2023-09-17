from classes import Wine, VVWine, TFWine,PolWine
import requests
import bs4 as bs
from urllib.parse import quote
import re
import datetime
import pytz
import os 
import pickle
import json

# Matching
import difflib
from thefuzz import fuzz
from thefuzz import process
import jaro

# from typing import Dict
import time

# Temp
def extract_subset(dictionary, n):
    return {key: value for index, (key, value) in enumerate(dictionary.items()) if index < n}


# Pol to TF mappings

#Polet: 'Israel', 'Armenia', 'Serbia', 'Bulgaria', 'Mexico', 'Colombia', 'Ecuador', 'Palestina', 'Belgia', 'Georgia', 
# 'Italia', 'St. Kitts og Nevis', 'Slovenia', 'Ukraina', 'Martinique', 'Tunisia', 'Syria', 'Ungarn', 'El Salvador', 'Peru', 
# 'Australia', 'Bermuda', 'Spania', 'Bosnia og Hercegovinia', None, 'Guatemala', 'Slovakia', 'Mauritius', 'Hellas', 'Vestbredden', 
# 'Haiti', 'Luxemburg', 'Venezuela', 'Sverige', 'Nederland', 'St. Lucia', 'Dominikanske republikk', 'Thailand', 'Japan', 'Island',
#  'Nicaragua', 'Indonesia', 'Uruguay', 'Taiwan', 'Chile', 'Trinidad og Tobago', 'Tyskland', 'Panama', 'Den europeiske union',
#  'Canada', 'Latvia', 'Romania', 'Sveits', 'Barbados', 'Frankrike', 'Marokko', 'Irland', 'England', 'India', 'Kosovo', 'Skottland',
#  'Golanhøydene', 'Jamaica', 'Estland', 'New Zealand', 'Norge', 'Danmark', 'Amerikanske Jomfruøyene', 'Tsjekkia', 'Puerto Rico', 'Cuba',
#  'Nord-Makedonia', 'USA', 'Storbritannia', 'Argentina', 'Sør-Afrika', 'Brasil', 'Kina', 'Polen', 'Guyana', 'Finland', 'Portugal',
#  'Østerrike', 'Kroatia', 'Republikken Moldova', 'Tyrkia', 'Libanon'

# TF: None, 'Chile', 'Italy', 'Australia', 'United States', 'Norway', 'United Kingdom', 'USA', 'Spain', 'Portugal', 'Austria', 'Germany', 'South Africa', 
# 'Hungary', 'Argentina', 'France', 'Lebanon', 'New Zealand'

country_EN_to_NO = {
    'Chile': 'Chile',
    'Italy': 'Italia',
    'Australia': 'Australia',
    'United States': 'USA',
    'USA': 'USA',
    'Norway': 'Norge',
    'United Kingdom': 'Storbritannia',
    'Spain':'Spania',
    'Portugal':'Portugal',
    'Austria':'Østerrike',
    'Germany':'Tyskland',
    'South Africa':'Sør-Afrika',
    'Hungary': 'Ungarn',
    'Argentina':'Argentina',
    'France':'Frankrike',
    'Lebanon':'Libanon',
    'New Zealand':'New Zealand',
    None: None #These values need to be checked to see if there it something that can be done about it! 
}
country_NO_to_EN = {v: k for k, v in country_EN_to_NO.items()}

#Polet: 'Øl', 'Rødvin', 'Rosévin', 'Hvitvin', 'Sake', 'Sider', 'Aromatisert vin', 'Mjød', 'Musserende vin', 'Fruktvin', 'Perlende vin', 'Brennevin', 'Sterkvin' #Sterkvin e vin meed 15-22% alkohol.
# TF: None, 'Sparkling Wine', 'Rosé Wine', 'White Wine', 'Red Wine', 'Champagne'
# Is this TF wine tags correct? Should be a bit different, no? 
# type_NO_to_EN = {
#     'Musserende vin': 'Sparkling Wine',
#     'Rosévin': 'Rosé Wine',
#     'Hvitvin': 'White Wine',
#     'Rødvin': 'Red Wine',
#     'key3': 'Champagne'
# }
# type_EN_to_NO = {v: k for k, v in type_NO_to_EN.items()}

# Burde egentlig vere: 
# TF: Rødvin, Perlende vin, Musserende vin, Hvitvin, Rosévin, Dessertvin, Hetvin
# Musserende vin -> Cava, Champagne, Cremant, Perlende vin, Prosecco
# TF = Rødvin, Hvitvin, Rosévin, Musserende vin, Hetvin, Brennevin
# Polet: Rødvin, Hvitvin, Rosévin, Sterkvin, Perlende vin, Aromatisert vin, Fruktvin[X], Musserende vin 

type_TF_to_POL = {
    'Musserende vin': ['Aromatisert vin', 'Musserende vin', 'Perlende vin'], #'Fruktvin' ??
    'Rosévin': ['Rosévin'],
    'Hvitvin': ['Hvitvin'],
    'Rødvin': ['Rødvin'],
    'Hetvin': ['Sterkvin']
}

# rapidfuzz.utils.default_process
# from rapidfuzz.utils import default_process
# str2 = default_process("test__12?!/gagw")
# print(str2)

def get_nested_attribute(data, *keys, default=None):
    """
    Get a nested attribute from a dictionary, handling empty values.
    :param data: The dictionary to traverse
    :param keys: Sequence of keys to access the nested attribute
    :param default: Default value to return if attribute is missing or empty
    :return: The attribute value or default value
    """
    for key in keys:
        data = data.get(key, {})
        if not data:  # Check for empty value
            return default
    return data

def match_tf_wines():
    # 1. Load and or generate new master wine dictionary
    # 2. Insert tax_free product as ground truth
    # 3. Load all pol products depending on category? And check closest match to the one from the tax free.
    # 4. Insert the pole wines with a higher rating than the threshold
    # 5. Search in vivino for similar product, check similarity and if above threshold, add this wine.
    # 6. Double check a lot of the wines manually to see if they are actually close to one another.

    # Load all specific wine dictionaries
    # Get data folder from this direction path
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_folder, exist_ok=True)

    #### Load master data dict ####
    pickle_file_path = os.path.join(data_folder, 'wines_master.pkl')
    try:
        with open(pickle_file_path, "rb") as file:
            master_dict = pickle.load(file)
    except FileNotFoundError:
        print("File does not exist.")
        master_dict = {} # db.storage.json.get("gameDayInfo.json")
    except pickle.PickleError as e:
        print(f"An error occurred with the pickle file: {e}")

    
    ##### Load tax free dict ####
    pickle_file_path = os.path.join(data_folder, 'TF.pkl')
    try:
        with open(pickle_file_path, "rb") as file:
            tf_dict = pickle.load(file)
    except FileNotFoundError:
        print("TF file does not exist. Return since it is needed to generate the winesmaster")
        return
    except pickle.PickleError as e:
        print(f"An error occurred with the pickle file: {e}")

    ##### Load pol dict ####
    pickle_file_path = os.path.join(data_folder, 'polet.pkl')
    try:
        with open(pickle_file_path, "rb") as file:
            pol_dict = pickle.load(file)
    except FileNotFoundError:
        print("TF file does not exist. Return since it is needed to generate the winesmaster")
        return
    except pickle.PickleError as e:
        print(f"An error occurred with the pickle file: {e}")
    
    print(f"All files loaded successfully to match the wines")



    # #### GET UNIQUE CATEGORIES AND COUNTRIES FROM TAX FREE DICT ####
    # # Check unique values of the data: 
    # unique_main_categories = set()
    # unique_countries = set()
    # dict = tf_dict
    # for wine in dict.values():
    #     # # Polet:
    #     # unique_countries.add(wine._main_country)
    #     # unique_main_categories.add(wine._main_category['name'])
    #     # Taxfree:
    #     unique_countries.add(country_EN_to_NO[wine._country])
    #     unique_main_categories.add(wine._category)
    # # print(f"Data info with {len(dict)} entries.")
    # # print(f"Unique main categories: {unique_main_categories}")
    # # #Polet: 'Øl', 'Rødvin', 'Rosévin', 'Hvitvin', 'Sake', 'Sider', 'Aromatisert vin', 'Mjød', 'Musserende vin', 'Fruktvin', 'Perlende vin', 'Brennevin', 'Sterkvin' #Sterkvin e vin meed 15-22% alkohol.
    # # # TF: None, 'Sparkling Wine', 'Rosé Wine', 'White Wine', 'Red Wine', 'Champagne'
    # # print(f"Unique countries: {unique_countries}")
    # # #Polet: 'Israel', 'Armenia', 'Serbia', 'Bulgaria', 'Mexico', 'Colombia', 'Ecuador', 'Palestina', 'Belgia', 'Georgia', 'Italia', 'St. Kitts og Nevis', 'Slovenia', 'Ukraina', 'Martinique', 'Tunisia', 'Syria', 'Ungarn', 'El Salvador', 'Peru', 'Australia', 'Bermuda', 'Spania', 'Bosnia og Hercegovinia', None, 'Guatemala', 'Slovakia', 'Mauritius', 'Hellas', 'Vestbredden', 'Haiti', 'Luxemburg', 'Venezuela', 'Sverige', 'Nederland', 'St. Lucia', 'Dominikanske republikk', 'Thailand', 'Japan', 'Island', 'Nicaragua', 'Indonesia', 'Uruguay', 'Taiwan', 'Chile', 'Trinidad og Tobago', 'Tyskland', 'Panama', 'Den europeiske union', 'Canada', 'Latvia', 'Romania', 'Sveits', 'Barbados', 'Frankrike', 'Marokko', 'Irland', 'England', 'India', 'Kosovo', 'Skottland', 'Golanhøydene', 'Jamaica', 'Estland', 'New Zealand', 'Norge', 'Danmark', 'Amerikanske Jomfruøyene', 'Tsjekkia', 'Puerto Rico', 'Cuba', 'Nord-Makedonia', 'USA', 'Storbritannia', 'Argentina', 'Sør-Afrika', 'Brasil', 'Kina', 'Polen', 'Guyana', 'Finland', 'Portugal', 'Østerrike', 'Kroatia', 'Republikken Moldova', 'Tyrkia', 'Libanon'
    # # # TF: None, 'Chile', 'Italy', 'Australia', 'United States', 'Norway', 'United Kingdom', 'USA', 'Spain', 'Portugal', 'Austria', 'Germany', 'South Africa', 'Hungary', 'Argentina', 'France', 'Lebanon', 'New Zealand'



    #### GET STRUCTURED ENTRY DATA FROM POLET DICT ####
    # This consist of the name corresponding to the wine (could add whatever is needed into this name)
    # TODO: Need to actually store the key too here, so instead of appending this name, might need to add a sub dict inside the array!
    pol_subset = pol_dict # extract_subset(pol_dict, 500)

    # TODO: Maybe pre-processing step for all entries in this dict to make the name reflect something more easily searchable such as adding the volume, year, district if present?
    # TODO: Do I need to do the same for the TF product such that they are actually similar? 

    # Organize wines into a structured dictionary
    structured_data = {}
    json_data = {}
    for wine_key, wine in pol_subset.items():
        wine_type = wine._main_category#['name'] #'name' needs to be removed once need data is gotten
        country = wine._main_country
        name = wine._name # Have a second look into this one, might need somethig els?
        # TODO: Add the actual tag to this wine somehow instead of using this "internal" name..
        
        # structured_data.setdefault(wine_type, {}).setdefault(country, []).append(name)
        # structured_data.setdefault(wine_type, {}).setdefault(country, {}).setdefault(name, [])#.append(wine)
        # structured_data.setdefault(wine_type, {}).setdefault(country, {name:wine_key})#.append({name:wine_key})
        structured_data.setdefault(wine_type, {}).setdefault(country, {})[name] = wine_key
        # .setdefault(name, {'full': wine_key}) #Change from single list too dict with key val .append([name,wine_key]) # TODO: TEST THIS! 

    #     json_data[wine_key] = wine.to_dict()
    
    # file_path = os.path.join(data_folder, 'pol.json')
    # with open(file_path, "w") as file:
    #     json.dump(json_data, file)

    # # Print the structured data from polet
    # for wine_type, countries in structured_data.items():
    #     print("Wine Type:", wine_type)
    #     for country, names in countries.items():
    #         print("  Country:", country)
    #         print("    Names:", names) 
    #     print() 

    # Get tax free subset and do comparison!
    tf_subset = extract_subset(tf_dict,50)

    # Initiate main dict of main wines.
    # TODO: Add load old main Wines here.
    wines_main = {}

    for tf_wine_name, tf_wine in tf_subset.items():
        #Initiate a new main Wine object. Might be better to just stick with the different objects and then just map them instead? 
        main_wine = Wine()
        main_wine.name = tf_wine_name
        main_wine.taxfree_wine = tf_wine

        # Check if type is available, otherwise move on, hence pol wine will be None
        # TODO: Add mapping of type such that subset is extracted! 

        # Get list of mappen wine types from TF to polet since polet are using more categories..
        wine_types = type_TF_to_POL[tf_wine._category]
        possible_matches = {}
        for wine_type in wine_types:
            try:
                wines_type = structured_data[tf_wine._category]
            except KeyError as e:
                print(f"Wine type key {tf_wine._category} does not seem to exist in the dataset: {e}")
                pass

            try:
                wines_country = wines_type[country_EN_to_NO[tf_wine._country]]
            except KeyError as e:
                print(f"Wine country {tf_wine._country} does not seem to exist in the dataset: {e}")
                pass
            possible_matches.update(wines_country)
        possible_name_matches_polet = possible_matches


        #### GET POSSIBLE MATCHES DEPENDING ON COUNTRY AND TYPE ####
        # If both of these have passed, then we could have a possible match within the subset! 

        probable_matches = {}

        # TODO: Need to try and full in year into all names used for identification! Makes everything a lot simpler.
        # NB NB NB! Do this for both TF and Polet. Polet does not have extra yearly info unfrotunately, so fine now for just adding on the TF side.
        # However, might want to add the bottle size on both sides :P

        tf_wine_name_searchable = tf_wine_name.split("-_-")[0]
        # Add year to TF name if missing
        if str(tf_wine._year) not in tf_wine_name_searchable and tf_wine._year != None:
            tf_wine_name_searchable = tf_wine_name_searchable + " " + str(tf_wine._year)
        # Add brand to TF name if missing
        if tf_wine._brand not in tf_wine_name_searchable and tf_wine._brand != None:
            tf_wine_name_searchable = tf_wine._brand + " " + tf_wine_name_searchable

        # Add size? (i.e. _amount) too for both TF and polet wines to find the correct bottle size?
        stop_words = ["Bag in box"]
        for word in stop_words:
            if word in tf_wine_name_searchable:
                tf_wine_name_searchable = tf_wine_name_searchable.replace(word,"")

        # Tests
        for possible_match, wine_key in possible_name_matches_polet.items():
            ratio = difflib.SequenceMatcher(None, possible_match.lower(), tf_wine_name_searchable.lower()).ratio()
            if ratio > 0.8:
                # probable_match.append(possible_match) # TODO: Add year or something too? 
                # probable_match_rating.append(ratio)
                probable_matches[possible_match] = {'similarity': ratio, 'key': wine_key} # TODO: End with this one maybe? 
        
        print(f"Possible matches for {tf_wine_name_searchable}: {probable_matches} using difflib.")
        # probable_match = []
        # probable_match_rating = []
        
        # # Fuzzy matching 2.0    
        # for possible_match in possible_name_matches_polet:
        #     ratio = fuzz.token_set_ratio(possible_match.lower(), tf_wine_name_searchable)
        #     if ratio > 75:
        #         probable_match.append(possible_match)
        #         probable_match_rating.append(ratio)
        # print(f"Possible matches for {tf_wine_name}: {probable_match} and ratings {probable_match_rating} using fuzz")
        # probable_match = []
        # probable_match_rating = []

        # OR! Could do this directly in one process it seems!
        probable_match_dict = process.extractBests(tf_wine_name_searchable, possible_name_matches_polet.keys(), limit=5, score_cutoff=80, scorer=fuzz.token_set_ratio)
        print(f"Probable matches for {tf_wine_name_searchable}: {probable_match_dict} using toket_set_ratio")
        # probable_match_dict2 = process.extractBests(tf_wine_name_searchable, possible_name_matches_polet.keys(), limit=5, score_cutoff=80, scorer=fuzz.token_sort_ratio)
        # print(f"Probable matches for {tf_wine_name_searchable}: {probable_match_dict2} using other")
        print()

        # TODO: Compare the two different methods to actually just store 1, 2 or 3? 




        # # TODO: IF THERE IS AN ACTUAL HIT:
        # # main_wine.polet_wine = pol_dict[probable_match] # Could be a list of these wines maybe? 
        # # for wines in possible_match: #Like this?
        # last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        # last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
        # main_wine.last_updated = last_updated_string


        # Fuzzy matching using Jaro. Does not seem to be a good fit for this purpose unfortunately! Too similar when nowhere near close to the actual product!
        # for possible_match in possible_name_matches_polet:
        #     # ratio = fuzz.token_set_ratio(possible_match.lower(), tf_wine_name.split("-_-")[0].lower())
        #     ratio = jaro.jaro_winkler_metric(tf_wine_name.split("-_-")[0].lower(),possible_match.lower())
        #     if ratio > 0.75:
        #         probable_match.append(possible_match)
        #         probable_match_rating.append(ratio)
        
        # print(f"Possible matches for {tf_wine_name}: {probable_match} and ratings {probable_match_rating} using jaro")
        # probable_match = []
        # probable_match_rating = []


    pickle_file_path = os.path.join(data_folder, 'WINES.pkl')
    with open(pickle_file_path, "wb") as file:
        pickle.dump(tf_dict, file)

    


def get_single_product_vivino(wine : Wine):
    wine_name = "Gran Feudo Crianza 2017"
    if wine.wine_name != None:
        wine_name = wine.wine_name
    else:
        print("Wine name of main object is None. -> Return")
        return
    
    # Check if there is already a VVWine in the object
    if wine.taxfree_wine._name != None:
        vv_wine = wine.taxfree_wine
    else:
        vv_wine = VVWine()

    
    # Search for a single product on vivino using the search functionality
    wine_name = wine_name.replace("\\s+", "+")
    encoded_name = quote(wine_name)
    wine_search_url = f"https://www.vivino.com/search/wines?q={encoded_name}"

    print(wine_search_url)
    headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    }
    r = requests.get(wine_search_url, headers=headers)
    if r.status_code == 200:
        html_content = r.text
        soup = bs.BeautifulSoup(html_content, 'html.parser')
        # soup = bs(r.content.decode('utf-8'),features="html.parser") 

        # We only try the first result
        first_card = soup.select_one('.card')
        # print(first_card)
        wine = first_card.select_one('.bold').get_text(strip=True)
        #print(wine)
        
        ##### TODO: Find these variables! 
        # wine_name =  #Find the wine name that is actually the hit of the first search too.
        # wine_match = 
        # TODO: Add a matcher to see how close match they are? 

        wine_rating = float(first_card.select_one('.average__number').get_text(strip=True))

        wine_no_ratings =  first_card.select_one('.text-micro').get_text(strip=True)
        wine_no_ratings_int = int(wine_no_ratings.strip('ratings'))

        # wine_url =  first_card.select_one('div.wine-card__image-wrapper a').get_text(strip=True)
        wine_url = first_card.select_one('div.wine-card__image-wrapper a')['href']
        full_wine_url = f"https://www.vivino.com{wine_url}"

        wine_image_wrapper = first_card.select_one('.wine-card__image-wrapper a')
        if wine_image_wrapper:
            wine_image_url = wine_image_wrapper.find('figure')['style']
            image_url_match = re.search(r'url\(//images\.vivino\.com.*\)', wine_image_url)
            
            if image_url_match:
                wine_image_url = image_url_match.group(0)[5:-1]  # Remove 'url(' and ')' from the match
                print(wine_image_url)
            else:
                print("Image URL not found")
        else:
            print("Image wrapper not found")
    
        # Insert information into object
        # vv_wine._wine_name = wine_name
        last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")

        vv_wine._last_updated = last_updated_string# datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        # vv_wine._match_percentage = wine_match
        vv_wine._rating = wine_rating  
        vv_wine._no_of_ratings = wine_no_ratings_int
        vv_wine._wine_url = wine_url
        vv_wine._full_wine_url = full_wine_url
        vv_wine._image_url = wine_image_url

    else:
        print(f"Request to {wine_search_url} failed with status code {r.status_code}")


def get_vivino_for_tf():
    print("TEST")


def get_all_taxfree(): #(wines : Dict[str,Wine]):
    # #wine_name = "Gran Feudo Crianza 2017"
    # if wine.wine_name != None:
    #     print(f"Tax free wine is already covered: {wine.wine_name}")
    #     return
    
    # # Otherwise, fill the tax free objects.
    # # TODO: Do I need like an update for example for the price if it say a discount or change in price or anything? 
    # tf_wine = TFWine()

    # Get data folder from this direction path
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_folder, exist_ok=True)

    # Reload the pickled file and unpickle the dictionary
    pickle_file_path = os.path.join(data_folder, 'TF.pkl')
    try:
        with open(pickle_file_path, "rb") as file:
            tf_dict = pickle.load(file)
    except FileNotFoundError:
        print("File does not exist.")
        tf_dict = {} # db.storage.json.get("gameDayInfo.json")
    except pickle.PickleError as e:
        print(f"An error occurred with the pickle file: {e}")


    # TODO: Is this allowed to use actually? Maybe I need to check or something for the application ID and so on and not use this directly?
    # Surely they dont have an actual API do they? 
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
    
    for page_nr in range(1, pages+1):
        time.sleep(1.5) # sleep for 500 ms such that I do not call the API too much.
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
            
            try:
                name = hit['name']['en'] 
            except KeyError as e:
                # if e == KeyError:
                    name = hit['name']['no']
                # else: 
                #     print("Need another one.")

            #TODO: NB! Mangla ca 100 vin. Trur det e pga duplikat keys mtp samme navn brukt fleire plassa! 
            product_code = hit['code']
            name = name + "-_-" + product_code

            categories = hit['allCategoriesNames']['no'] # NB! Some weird results for this. Some are placed as Other even though they DO have a well defined type for exaple rosé wine...
            # TODO: Like so godt å hente dette på norsk kanskje!? IOM at kategorien er på norsk fra polet anyways? 
            
            #no: Rosévin, Likør [Brennevin], Rødvin, Hvitvin, Brennevin, Cremant [Musserende vin], Øl, Prosecco [Musserende vin], Champagne [Musserende vin], Dessertvin [Musserende vin]
            # Cava [Musserende vin], Dessertvin, Hetvin, Perlende vin/Dessertvin/Musserende vin, 

            
            if "Hvitvin" in categories:
                category = "Hvitvin"
            elif "Rødvin" in categories:
                category = "Rødvin"
            elif "Rosévin" in categories:
                category = "Rosévin"
            elif "Musserende vin" in categories:
                category = "Musserende vin"
            elif "Øl" in categories:
                category = "Øl"
            elif "Hetvin" in categories:
                category = "Hetvin"
            elif "Brennevin" in categories:
                category = "Brennevin"
            else:
                category = "Annen"

            
            # categories = hit['allCategoriesNames']['en']
            # if 'Whitewine' in categories:
            #     category = 'Whitewine'
            # elif 'Redwine' in categories:
            #     category = 'Redwine'
            # elif 'Sparkling wine' in categories:
            #     category = 'Sparkling wine'
            # elif 'Roséwine' in categories:
            #     category = 'Roséwine'
            # elif 'Beer' in categories:
            #     category = 'Beer'
            # elif 'Spirits' in categories:
            #     category = 'Spirits'
            # else:
            #     category = 'Other'

            # # Only store the wines atm, so discard the rest :P
            # if category not in ['Other', 'Beer', 'Sprits']:
            # Only store the wines atm, so discard the rest :P
            if category not in ['Annen', 'Øl', 'Brennevin']:
                # Get unit, could have used the function.
                if 'en' in hit['unit'].keys():
                    unit = hit['unit']['en']
                elif 'no' in hit['unit'].keys():
                    unit = hit['unit']['no']

                product_url = "https://www.tax-free.no/no" + hit['url']
                nameExtended = hit['erpName']['en'] # Do we need to have multiple of this one too? 
                country_default = None
                if (get_nested_attribute(hit,'country','en')== None):
                    # print(f"Something wrong with this wine with country with value: {hit['country']}")
                    if ("Portvin" in categories):
                        country_default = "Portugal"

                if name not in tf_dict:
                    try:
                        tf_wine = TFWine()
                        tf_wine._name = name
                        tf_wine._name_extended = nameExtended
                        tf_wine._country = get_nested_attribute(hit,'country','en',default=country_default)# hit['country']['en'] if hit["country"] is not None else "-"
                        
                        tf_wine._area = get_nested_attribute(hit,'wineCultivationArea','en') #hit['wineCultivationArea']['en'] if hit["wineCultivationArea"] is not None else "-"
                        tf_wine._categories = categories
                        tf_wine._category = category
                        tf_wine._type = get_nested_attribute(hit, 'wineType','en')
                        tf_wine._year = get_nested_attribute(hit,'year','en') # hit['year']['en'] if hit["year"] is not None else "-"
                        tf_wine._brand = get_nested_attribute(hit,'brandName', 'no') #hit['brandName']['no'] if hit["brandName"] is not None else "-"
                        tf_wine._amount = float(hit['salesAmount']) if hit['salesAmount'] is not None else 0
                        tf_wine._unit = unit
                        tf_wine._description = get_nested_attribute(hit, 'description', 'en') #hit['description']['en'] if hit["description"] is not None else "-"
                        tf_wine._alc_percentage = get_nested_attribute(hit, 'alcoholByVolume') #hit['alcoholByVolume'] if hit['alcoholByVolume'] is not None else "-"
                        tf_wine._grapes = get_nested_attribute(hit, 'wineGrapes','en') #hit['wineGrapes']['en'] if hit["wineGrapes"] is not None else "-"
                        tf_wine._url = product_url
                        tf_wine._availability = get_nested_attribute(hit, 'availableIn')
                        tf_wine._availability_airports = get_nested_attribute(hit, 'availableInAirportCodes')
                        tf_wine._in_stock_in = get_nested_attribute(hit, 'inStockIn')

                        update_values = {"NOK" : hit['price']['NOK'],
                                        "EUR" : hit['price']['EUR']}
                        tf_wine.update_price(update_values)

                        last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                        last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                        tf_wine._last_updated = last_updated_string


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
                    except Exception as e:
                        # Catch the exception and print its details
                        print(f"An exception occurred when loading data for {tf_wine._name} with exception: {str(e)}")
                    
                else:
                    # Wine is already in the taxfree DF. handle the stuff, mainly updating prices I believe.
                    tf_wine = tf_dict[name] 
                    last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                    last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                    update_values = {"NOK" : hit['price']['NOK'],
                                        "EUR" : hit['price']['EUR']}
                    tf_wine.update_price(update_values)
                    tf_wine._last_updated = last_updated_string
                
                tf_dict[name] = tf_wine

            # else:
            #     print(f"Category: {category} not currently supported for product {name}. Discard!")

    pickle_file_path = os.path.join(data_folder, 'TF.pkl')
    with open(pickle_file_path, "wb") as file:
        pickle.dump(tf_dict, file)

    
def get_all_polet():

    wine_types = [':mainCategory:rødvin', ':mainCategory:hvitvin', ':mainCategory:musserende_vin', ':mainCategory:rosévin', ':mainCategory:perlende_vin', ':mainCategory:aromatisert_vin', ':mainCategory:fruktvin']

    # Get data folder from this direction path
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_folder, exist_ok=True)

    # Reload the pickled file and unpickle the dictionary
    pickle_file_path = os.path.join(data_folder, 'polet.pkl')
    try:
        with open(pickle_file_path, "rb") as file:
            pol_dict = pickle.load(file)
    except FileNotFoundError:
        print("File does not exist.")
        pol_dict = {} # db.storage.json.get("gameDayInfo.json")
    except pickle.PickleError as e:
        print(f"An error occurred with the pickle file: {e}")


    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Content-Type": "application/json"
    }
    wines_pr_page = 24 #max 1k previously
    page = 0
    for wine_type in wine_types:
   
        # Request first page to figute out how many pages we ought to scrape.
        # url = f"https://www.vinmonopolet.no/api/search?q=:relevance&searchType=product&currentPage={page}&fields=FULL&pageSize={wines_pr_page}"
        # url = f"https://www.vinmonopolet.no/search/?q=:relevance&searchType=product&currentPage={page}" #&fields=FULL&pageSize={wines_pr_page}"
        url = f"https://www.vinmonopolet.no/vmpws/v2/vmp/search?q=:relevance{wine_type}&searchType=product&currentPage={page}&fields=FULL&pageSize={wines_pr_page}"
        print(f"Processing page nr {page} with url: {url}.")

        try:
            r = requests.get(url, headers=headers)
            page_json = r.json()  # Parse JSON content
            # page_json = json.loads(r.text)
        except Exception as e:
            print(f"Something went wrong when reading the first page. Error: {e}. \nProbably need to revisit the scraping!")
            return
        page_product_list = page_json['productSearchResult']['products']
        page = page_json['productSearchResult']['pagination']['currentPage']
        page_size = page_json['productSearchResult']['pagination']['pageSize']
        pages = page_json['productSearchResult']['pagination']['totalPages']
        total_results = page_json['productSearchResult']['pagination']['totalResults']


        for page_nr in range(0, pages+1):
            # Sleep for a second before moving on to next page such that the requests are not exceeded.
            time.sleep(1.5)

            # url = f"https://www.vinmonopolet.no/api/search?q=:relevance&searchType=product&currentPage={page_nr}&fields=FULL&pageSize={wines_pr_page}"
            url = f"https://www.vinmonopolet.no/vmpws/v2/vmp/search?q=:relevance{wine_type}&searchType=product&currentPage={page_nr}&fields=FULL&pageSize={wines_pr_page}"
            print(f"Processing page nr {page_nr}/{pages}.")

            r = requests.get(url, headers=headers)
            page_json = json.loads(r.text)
            page_product_list = page_json['productSearchResult']['products']

            for product in page_product_list:
                # pprint.pprint(product)

                # product_url = 'https://www.vinmonopolet.no' + product['url']
                volume = product['volume']['value']
                product_name = product['name']


                # Define name with category and size because there could be different sizes of the same products!
                # name = product['name'] + " " + str(volume) + "L" + " " + product['main_category']['name'] #Maybe need to add the category since some people are just useless.. using the same name for all different wines..
                # TODO: Check if there are multiple of the same products with same size too, if that is the case, we might have to use the product ID to distinguish in between the instances.
                product_code = product['code']
                name = product['name'] + "-_-" + product_code # IF product code is used here, then all entries will be different, hence, there will be NO LOCAL BARGAIN actually
                # TODO: Is this really the desired behaviour? 

                availability_text = product['availability']['storeAvailability']['mainText']
                if availability_text != 'Utsolgt i alle butikker - kan ikke bestilles':
                    if name not in pol_dict:
                        #Check availability if it is worth handling this wine or if it is deprecated.
                        pol_wine = PolWine()
                        volume = get_nested_attribute(product,'volume', 'value')
                        pol_wine._name = product_name #+ ' ' + volume if volume is not None else product_name # Adding the volume to the product name if available
                        #TODO: Worth it to add district if existent etc? 
                        # TODO: Maybe this should be done in a more proper ETL way :p Just extract ALL data and then fix it in a medallion structure from thereon!!
                        pol_wine._name_extended = name
                        # pol_wine._main_country = product['main_country']['name'] if product["main_country"] is not None else None
                        # pol_wine._district = product['district']['name'] if product["district"] is not False else None
                        pol_wine._district = get_nested_attribute(product, 'district','name', default=None)#product.get('district', {}).get('name', None)
                        pol_wine._main_country = get_nested_attribute(product, 'main_country','name', default=None)

                        pol_wine._main_sub_category = get_nested_attribute(product, 'main_sub_category') # product['main_sub_category'] if product["main_sub_category"] is not None else None
                        pol_wine._main_category = get_nested_attribute(product, 'main_category','name')#product['main_category'] if product["main_category"] is not None else None
                        pol_wine._volume = volume #product['volume']['value'] if product["volume"] is not None else None
                        price = get_nested_attribute(product, 'price', 'value') #product['price']['value'] if product["price"] is not None else None
                        pol_wine.update_price(price)
                        pol_wine._expired = get_nested_attribute(product, 'expired') #product['expired'] if product["expired"] is not None else None
                        pol_wine._buyable = get_nested_attribute(product, 'buyable')#product['buyable'] if product["buyable"] is not None else None
                        pol_wine._url = 'https://www.vinmonopolet.no' + product['url']
                        pol_wine.set_availability(availability_text)
                        pol_wine._code = product_code

                        last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                        last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                        pol_wine._last_updated = last_updated_string

                    else:
                        # Instance already exits in dict. Update values.
                        
                        # TODO: Need to fix this shit with multiple entries of the same goddamn wine. Could be nice to find bargains at Polet!
                        # https://www.vinmonopolet.no/search?q=Ch.%20L%C3%A9oville%20Poyferr%C3%A9%202018:relevance&searchType=product
                        # Maybe add extra layer for checking availability compared to price, so could have
                        # {best_online_price: X, best_local_price: Y, price_diff: Z}
                        pol_wine = pol_dict[name]
                        price = get_nested_attribute(product, 'price', 'value') #product['price']['value'] if product["price"] is not None else None
                        pol_wine.update_price(price)
                        pol_wine._expired = get_nested_attribute(product, 'expired') #product['expired'] if product["expired"] is not None else None
                        pol_wine._buyable = get_nested_attribute(product, 'buyable')#product['buyable'] if product["buyable"] is not None else None
                        pol_wine._url = 'https://www.vinmonopolet.no' + product['url']
                        pol_wine.set_availability(availability_text)

                        last_updated = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                        last_updated_string = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                        pol_wine._last_updated = last_updated_string
                    
                    pol_dict[name] = pol_wine
                else:
                    print('Discard entry due to NO availability for product {product_name}') 
    
    pickle_file_path = os.path.join(data_folder, 'polet.pkl')
    with open(pickle_file_path, "wb") as file:
        pickle.dump(pol_dict, file)

            

def get_vivino(wine_name : str):
    wine_name = wine_name.replace("\\s+", "+")
    encoded_name = quote(wine_name)
    wine_search_url = f"https://www.vivino.com/search/wines?q={encoded_name}"

    print(wine_search_url)
    headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    }
    r = requests.get(wine_search_url, headers=headers)
    if r.status_code == 200:
        html_content = r.text
        soup = bs.BeautifulSoup(html_content, 'html.parser')
        # soup = bs(r.content.decode('utf-8'),features="html.parser") 

        # We only try the first result
        first_card = soup.select_one('.card')
        # print(first_card)
        wine = first_card.select_one('.bold').get_text(strip=True)
        #print(wine)

        wine_rating = first_card.select_one('.average__number').get_text(strip=True)

        wine_no_ratings =  first_card.select_one('.text-micro').get_text(strip=True)
        wine_no_ratings1 = int(wine_no_ratings.strip('ratings'))

        # wine_url =  first_card.select_one('div.wine-card__image-wrapper a').get_text(strip=True)
        wine_url = first_card.select_one('div.wine-card__image-wrapper a')['href']
        full_wine_url = f"https://www.vivino.com{wine_url}"

        wine_image_wrapper = first_card.select_one('.wine-card__image-wrapper a')
        if wine_image_wrapper:
            wine_image_url = wine_image_wrapper.find('figure')['style']
            image_url_match = re.search(r'url\(//images\.vivino\.com.*\)', wine_image_url)
            
            if image_url_match:
                wine_image_url = image_url_match.group(0)[5:-1]  # Remove 'url(' and ')' from the match
                print(wine_image_url)
            else:
                print("Image URL not found")
        else:
            print("Image wrapper not found")
    else:
        print(f"Request to {wine_search_url} failed with status code {r.status_code}")