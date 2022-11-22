import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


""" Data manipulation """
# load on one dataset for spead Skip date
df = pd.read_csv(
    "Data/combined_data_1.txt", header=None, names=["Cust_Id", "Rating"], usecols=[0, 1]
)

df["Rating"] = df["Rating"].astype(float)

print("Dataset 1 shape: {}".format(df.shape))
print("-Dataset examples-")
print(df.iloc[::5000000, :])


""" Data cleaning """
df_nan = pd.DataFrame(pd.isnull(df.Rating))
df_nan = df_nan[df_nan["Rating"] == True]
df_nan = df_nan.reset_index()

movie_np = []
movie_id = 1

for i, j in zip(df_nan["index"][1:], df_nan["index"][:-1]):
    # numpy approach
    temp = np.full((1, i - j - 1), movie_id)
    movie_np = np.append(movie_np, temp)
    movie_id += 1

# Account for last record and corresponding length
# numpy approach
last_record = np.full((1, len(df) - df_nan.iloc[-1, 0] - 1), movie_id)
movie_np = np.append(movie_np, last_record)

print("Movie numpy: {}".format(movie_np))
print("Length: {}".format(len(movie_np)))

# remove those Movie ID rows
df = df[pd.notnull(df["Rating"])]

df["Movie_Id"] = movie_np.astype(int)
df["Cust_Id"] = df["Cust_Id"].astype(int)
print("-Dataset examples-")
print(df.iloc[::5000000, :])


""" Data slicing """
f = ["count", "mean"]

df_movie_summary = df.groupby("Movie_Id")["Rating"].agg(f)
df_movie_summary.index = df_movie_summary.index.map(int)
movie_benchmark = round(df_movie_summary["count"].quantile(0.7), 0)
drop_movie_list = df_movie_summary[df_movie_summary["count"] < movie_benchmark].index

print("Movie minimum times of review: {}".format(movie_benchmark))

df_cust_summary = df.groupby("Cust_Id")["Rating"].agg(f)
df_cust_summary.index = df_cust_summary.index.map(int)
cust_benchmark = round(df_cust_summary["count"].quantile(0.7), 0)
drop_cust_list = df_cust_summary[df_cust_summary["count"] < cust_benchmark].index

print("Customer minimum times of review: {}".format(cust_benchmark))

print("Original Shape: {}".format(df.shape))
df = df[~df["Movie_Id"].isin(drop_movie_list)]
df = df[~df["Cust_Id"].isin(drop_cust_list)]
print("After Trim Shape: {}".format(df.shape))
print("-Data Examples-")
print(df.iloc[::5000000, :])

df.to_csv("df.csv")
df_p = pd.pivot_table(df, values="Rating", index="Cust_Id", columns="Movie_Id")

print(df_p.shape)

df_p.head(10)