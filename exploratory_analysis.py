import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("df.csv")
del df["Unnamed: 0"]
df.head(5)

print("Number of unique users :", df.Cust_Id.nunique())
print("Number of unique ratings :", df.Rating.nunique())
print("Number of unique movies :", df.Movie_Id.nunique())

df_gp = df.groupby(by=["Rating"]).agg({"Cust_Id": "count"}).reset_index()
df_gp.columns = ["Rating", "Count"]

sns.barplot(x="Rating", y="Count", data=df_gp)

df_title = pd.read_csv(
    "Data/movie_titles.csv",
    encoding="ISO-8859-1",
    header=None,
    names=["Movie_Id", "Year", "Name"],
)
df_title.head(3)

print("Number of unique movies   :", df_title.Movie_Id.nunique())
print("Number of unique years    :", df_title.Year.nunique())
print("Number of unique titles   :", df_title.Name.nunique())