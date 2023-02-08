import requests
from bs4 import BeautifulSoup as bs
import re
from requests_html import HTMLSession
import unicodedata
import pandas as pd

#This script uses the traditional scraping method of getting the HTTP page, scrape the item entries that can be seen in the window.
# However, this solution is lacking capabilities such as getting the country, taste, district, year and so on...
# If this is to be scraped similarly, then I would need to move into the url, interact to view more and then scrape that full page too..
# Therefore, it is better to actually get the API calls right instead ang get info directly out from the API using a HTTPS GET request.

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
}
wines_pr_page = 50 #max 50!

data = []
# session = HTMLSession()
for page in range(1,2):
    url = f"https://www.tax-free.no/no/category941/alkohol?category=Alkohol&page={page}"
    print(f"Processing page nr {page} with url: {url}.")
    
    session = HTMLSession()
    response = session.get(url, headers=headers)
    response.html.render() # This is the critical line. This render method runs the script tags to turn them into HTML

    # Get the item SKU and Name from the html
    # Note: the "html.find()" method takes CSS selectors
    container = response.html.find("#product_list", first=True)
    list = container.find("li")
    for item in list:
        if 'class' in item.attrs:
            if 'list-item' in item.attrs['class']:
                product_url = next(iter(item.absolute_links))
                elements = item.text.split("\n")
                product_name = elements[3]
                product_producer = elements[4]
                product_volume = elements[5]
                product_price_string = unicodedata.normalize("NFKD", elements[6])
                product_price = float(product_price_string.split()[0])
                
                # TODO: Add additional request to get the actual product information for all products.
                # That will make it way easier to categorize and also make it more searchable. Probably need it to both connect to Vinmonopolet and Vivino.
                product_session = HTMLSession()
                product_r = product_session.get(product_url, headers=headers)
                # script = """
                # () => {
                # setTimeout(function(){
                #     document.querySelectorAll("a")[2].click();
                # }, 3000);
                # }
                # """
                # Does this actually work to expand all?
                
                # product_r.html.render(sleep=4,script="$(function() {return;});", reload=True ) # This is the critical line. This render method runs the script tags to turn them into HTML
                product_r.html.render()
                # Get the item SKU and Name from the html
                # Note: the "html.find()" method takes CSS selectors
                # product_container = product_r.html.find(".info-block-content", first=True) # class="features ng-star-inserted"
                # features1 = product_container.find(".product-feature")
                # ADD, this does not reach the expanded shitness, button needs to be clicked.. :TODO
                # Think I need either a script in the rendering oooor selenium with XPATH... which is probably best.
                # Or maybe it is actually best to o through the API or something :P
                features = product_r.html.find(".product-feature")
                print(len(features))
                for feature in features:
                    print(feature.text)
                
                # list = container.find("li")
                # Maybe something like this?? 
                # https://stackoverflow.com/questions/18597735/clicking-on-a-link-via-selenium
                
                data.append([product_name, product_producer, product_volume, product_price, product_price_string, product_url])

df = pd.DataFrame(data=data,columns=["Product", "Producer", "Volume", "Price", "Price_string", "URL"])    
print(df.head(30))