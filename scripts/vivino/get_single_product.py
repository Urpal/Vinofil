# Search for a single product on vivino using the search functionality
import requests
import bs4 as bs
from urllib.parse import quote
import re

wine_name = "Gran Feudo Crianza 2017"

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


get_vivino(wine_name)