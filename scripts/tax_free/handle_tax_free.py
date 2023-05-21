import pandas as pd 
import pickle as pickle
from pprint import pprint
# from vivino import get_vivino

tf_df = pd.read_pickle("data/tf.pickle")

# print(pprint(tf_df.head(10)))
# unique_categories = tf_df['Category'].value_counts().to_dict()
# print(unique_categories)
tf_df = tf_df[~(tf_df['Country'] == '-')] # Remove all entries that do not have a country.

unique_countries = tf_df['Country'].value_counts().to_dict()
print(unique_countries)

unique_categories = tf_df['Category'].value_counts().to_dict()
print(unique_categories)

json_data = tf_df.to_dict(orient='records')
# print(pprint(json_data[0]))

#Todo: Search vivino for wine types and specified countries.



for entry in json_data:
    pprint(entry)
