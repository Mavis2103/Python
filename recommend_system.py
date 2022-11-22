import turicreate
import pandas as pd
import numpy as np

df = pd.read_csv("df.csv")
# remove unnamed column
del df["Unnamed: 0"]
df.head(5)

# Recommendation with Collaborative Filtering (memory-based algorithm, it recommends items based on previous ratings)
# User-User collaborative filtering (users having higher correlation will tend to be similar.)
# Item-Item collaborative filtering (item/movies that are similar to each other are recommended)
df_sf = turicreate.SFrame(df)
# User-User: recommend top 5 movies based on the most popular choices (all the users receive the same recommendations)
# Training the model
popularity_model = turicreate.popularity_recommender.create(
    df_sf, user_id="Cust_Id", item_id="Movie_Id", target="Rating"
)

# Making recommendations (example) - print top 5 recommendations for the first 3 users
popularity_recomm = popularity_model.recommend(users=list(df_sf["Cust_Id"][0:3]), k=5)
popularity_recomm.print_rows(num_rows=15)

# Item-Item - recommend movies based on past personal preferences (different users will have a different set of recommendations - personalized recommendations)
# Training the model
item_sim_model = turicreate.item_similarity_recommender.create(
    df_sf,
    user_id="Cust_Id",
    item_id="Movie_Id",
    target="Rating",
    similarity_type="cosine",
)


# Making recommendations (example) - print top 5 recommendations for the first 3 users
item_sim_recomm = item_sim_model.recommend(users=list(df_sf["Cust_Id"][0:3]), k=5)
item_sim_recomm.print_rows(num_rows=15)


# user Cold Start - Making recommendations for a new user
# (it is not possible to provide personalized recommendations for a new user)

# If the model has never seen the user,
# then it defaults to recommending popular items
if sum(df_sf["Cust_Id"] == 12) == 0:
    print("The user 12 is new")
popularity_model.recommend(users=[12], k=5)


# item Cold Start - Making recommendations for a new item

# If the model has never seen the item,
# then it defaults to score = 0 (which is the minimum)
if sum(df_sf["Movie_Id"] == 12) == 0:
    print("The item 12 is new")
item_sim_model.recommend(users=list(df_sf["Cust_Id"][0:3]), items=[12])