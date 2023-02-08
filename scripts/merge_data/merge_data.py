import difflib
from functools import partial
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#Try using fuzzymatcher instead of difflib
import fuzzymatcher
import time

linked_df = pd.read_pickle("data/linked_df.pickle")
# print(linked_df.head(5))
print(f"Size of linked df: {linked_df.index}")
linked2 = linked_df[linked_df['best_match_score']>0.05]
print(f"Size of linked df: {linked2.index}")


#Load datas
#Test reading the same data
pol_df = pd.read_pickle("data/polet.pickle")
# print(pol_df.head(10))
# print(len(pol_df.index))
# Remove unnecessary shit from the dataframe that is not of interest.
pol_df = pol_df[pol_df['Category'].isin(['Brennevin','Rødvin','Hvitvin','Øl','Musserende vin','Sterkvin','Perlende vin'])] # list Category of interest
print(len(pol_df.index))
brennevin_df = pol_df[pol_df['Category'].isin(['Brennevin'])] # list Category of interest
print(len(brennevin_df.index))
rødvin_df = pol_df[pol_df['Category'].isin(['Rødvin'])] # list Category of interest
print(len(rødvin_df.index))
hvitvin_df = pol_df[pol_df['Category'].isin(['Hvitvin'])] # list Category of interest
print(len(hvitvin_df.index))
øl_df = pol_df[pol_df['Category'].isin(['Øl'])] # list Category of interest
print(len(øl_df.index))
musserende_df = pol_df[pol_df['Category'].isin(['Musserende vin'])]
print(len(musserende_df.index))
sterkvin_df =  pol_df[pol_df['Category'].isin(['Sterkvin'])]# list Category of interest
print(len(sterkvin_df.index))
perlende_df = pol_df[pol_df['Category'].isin(['Perlende vin'])] # list Category of interest
print(len(perlende_df.index))

# print(pol_df.head(10))
# print(len(pol_df.index))
print()

#Test reading the same data
tf_df = pd.read_pickle("data/tf.pickle")
# print(tf_df.head(10))
# tf_other = tf_df[tf_df['Category'] == 'Other']
# print(tf_other.index)
tf_df = tf_df[tf_df['Category'].isin(['Whitewine','Redwine','Beer','Spirits','Sparkling wine'])] #list Category of interest
print(len(tf_df.index))

brennevin_tf = tf_df[tf_df['Category'] == 'Spirits']
print(len(brennevin_tf.index))

hvitvin_tf = tf_df[tf_df['Category'] == 'Whitewine']
print(len(hvitvin_tf.index))
rødvin_tf = tf_df[tf_df['Category'] == 'Redwine']
print(len(rødvin_tf.index))
øl_tf = tf_df[tf_df['Category'] == 'Beer']
print(len(øl_tf.index))
musserende_tf = tf_df[tf_df['Category'] == 'Sparkling wine']
print(len(musserende_tf.index))

# Test using fuzzymatcher with specified columns. might need to change language of one of the DFs though..
df_left = rødvin_tf #tf_df
# column_names = ["ExtendedName","Name", "ProductID" ,"Country", "District" ,"Category", "SubCategory","Year", "Price", "OldPrice", "Volume", "LitrePrice", "OldLitrePrice", "Expired", "Buyable","BestLocalPrice","BestLocalLitrePrice","PossibleBargain","BestLocalURL", "URL"]
buyable_df = rødvin_df[rødvin_df["Buyable"] == True]
df_right = buyable_df #pol_df
# column_names = ["Name", "Country", "Area", "Category", "Year", "Brand",  "PriceNOK","PriceNOKOld", "LitrePriceNOK", "LitrePriceNOKOld", "PriceEUR", "PriceEUROld", "LitrePriceEUR", "LitrePriceEUROld", "Unit", "Amount", "Description", "Percentage", "Grapes", "URL"]
# Columns to match on from df_left
right_on = ["Name", "Country", "District", "Year"]

# Columns to match on from df_right
left_on = ["Name", "Country", "Area", "Year"]

import time

start = time.time()
# Note that if left_id_col or right_id_col are admitted a unique id will be autogenerated
linked_df = fuzzymatcher.fuzzy_left_join(df_left, df_right, left_on, right_on)  #, left_id_col = "id", right_id_col = "id")
end = time.time()
print(f"Elapsed time: {end - start} seconds.")
# print(df_left.head(5))
# print(df_right.head(5))
print(linked_df.head(5))
sorted_df = linked_df.sort_values('best_match_score', ascending=False)
print(sorted_df.head(10))
best_price_diff = 0
best_buy_id = 0
best_but_url = ""
for index, row in sorted_df.iterrows():
    print(f"Name left {row['Name_left']} vs {row['Name_right']} on right.")
    print(f"Country left {row['Country_left']} vs {row['Country_right']} on right.")
    print(f"District left {row['Area']} vs {row['District']} on right.")
    print(f"Yearleft {row['Year_left']} vs {row['Year_right']} on right.")
    print(f"match score: {row['best_match_score']}")
    if row['best_match_score'] > 0.05: # This seems to be an OK threshold, but it is not a 100% unfortunately..
        print('#### Match! ####') 
        price_diff = row['PriceNOK'] - row['Price']
        if price_diff > best_price_diff:
            best_price_diff = price_diff
            best_buy_id = row["__id_left"]
            best_but_url = row['URL_left']
    else: 
        print('#### This should NOT be a match ####')
    

    # print(row)
    print()
linked_df.to_pickle("data/linked_df.pickle")
# Maybe it is worth it to check something more? Or just take whatever is pretty much clear?


# This still takes forever. Taking the two full dfs equals something like
# 33k entries for polet and 903 entries for the taxfree => 903 * 33k = 29.8 million fuzzy mathes ffs! 

# Splitting the data up into smaller dfs will yield something like
# Redwine: 13300 * 244 = 3.245 mill => 224 seconds. if it is a linear relationship, then the last call probably took like 
# Whitewine: 8430  * 177 = 1.49
# Sparkling wine: 3300 * 123 = 0.406
# Spirits:  4602 * 356 = 1.64
# Beer: 3388 * 63 = 0.213
# Total => 7 mill, a drastically smaller amount of computations but still A LOT lol :P 




# # Remove unnecessary shit from the dataframe that is not of interest.
# tf_df = tf_df['']
# print(tf_df.head(10))
# print(len(tf_df.index))

# Todo. might need to have a stripped naming column for better  matching!
# THIS TAKES FOREVER
# f = partial(difflib.get_close_matches, possibilities=pol_df['Name'].tolist(), n=1)

# matches = tf_df['Name'].map(f).str[0].fillna('')
# scores = [
#     difflib.SequenceMatcher(None, x, y).ratio() 
#     for x, y in zip(matches, tf_df['Name'])
# ]

# tf_df.assign(best=matches, score=scores)
# print(tf_df.head(10))



# This will take forever! 
# difflib.get_close_matches("Hallo", words, len(words), 0)
# difflib.SequenceMatcher(None, odds_game.lower(), NT_game.lower()).ratio()
# ratios = [difflib.SequenceMatcher(None, odds_game.lower(), NT_game.lower()).ratio(), 
#         difflib.SequenceMatcher(None, home_team_stripped.lower(), heime_team_stripped.lower()).ratio(), 
#         difflib.SequenceMatcher(None, away_team_stripped.lower(), borte_team_stripped.lower()).ratio()]
# ratio = statistics.mean(ratios)

# tf_df['name_match'] = tf_df['Name'].apply(lambda x : difflib.get_close_matches(x, pol_df['Namew'].tolist(), cutoff=0.9))
# print(tf_df.head(10))