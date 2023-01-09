import sys
import pandas as pd
import psycopg2
import streamlit as st
import streamlit_nested_layout
import doc

# Read the result.csv file into a dataframe
data = pd.read_csv("Data/result.csv", engine="c")

# Cache the results of the out_similar() function to improve performance


@st.cache
def out_similar(movieid):
    '''get similar movie and related infos of the searched object; return none for exception'''
    result = data[data["0"] == movieid]
    for i in result.iterrows():
        index, output = i
        output = output.tolist()
        output[15] = output[15][1:-1].replace("'", "")
        output[18] = output[18][1:-1].replace("'", "").split(",")
        with open('Data/movielist.txt', 'r') as f:
            # Iterate over the lines in the file and append them to a list
            x = [line for line in f]
        output.append(x)
        return output

# Cache the results of the get_image() function to improve performance


@st.cache
def get_image(movieid):
    '''get image of the searched object'''
    sql = "SELECT movieimg FROM movieid WHERE movieid = %s LIMIT 1"
    with psycopg2.connect('dbname=movies user=postgres password=39339') as conn:
        # Read the image from the database
        img = pd.read_sql_query(sql, con=conn, params=(movieid,))
    return img['movieimg'][0]


# Config
st.set_page_config(layout='wide')

# Initialization


def clear():
    st.session_state['search_input'] = ''
    st.session_state['data'] = {
        "most_movie": {
            "id": "",
            "name": "",
            "genre": "",
            "image": "",
        },
        "most_review": {
            "content": "",
            "topic": ""
        },
        "movies_recommend": [],
    }


if 'search_input' not in st.session_state:
    clear()

search_input_value = st.session_state['search_input']

if search_input_value:
    clear()
    result = out_similar(search_input_value)
else:
    clear()
    result = out_similar("Hulk")

if result:
    st.session_state["data"]["most_movie"]["id"] = result[2]
    st.session_state["data"]["most_movie"]["name"] = result[1]
    st.session_state["data"]["most_movie"]["genre"] = result[15]
    st.session_state["data"]["most_movie"]["image"] = get_image(
        result[2])
    st.session_state["data"]["most_review"]["content"] = result[16]
    st.session_state["data"]["most_review"]["topic"] = result[17]
    st.session_state["data"]["movies_recommend"] = [
        {"id": result[4], "name":result[3],
         "image":get_image(result[4])},
        {"id": result[6], "name":result[5],
         "image":get_image(result[6])},
        {"id": result[8], "name":result[7],
         "image":get_image(result[8])},
        {"id": result[10], "name":result[9],
         "image":get_image(result[10])},
        {"id": result[12], "name":result[11],
         "image":get_image(result[12])},
        {"id": result[14], "name":result[13],
         "image":get_image(result[14])},
    ]


if search_input_value:
    st.header("Result for search: "+search_input_value)
col1, col2, col3 = st.columns([1, 3, 2])
if st.session_state["data"]["most_movie"]["image"]:
    with col1:
        st.image(st.session_state["data"]["most_movie"]["image"])

with col2:
    with st.form(key='search_form'):
        st.text_input("Movie name", key="search_input")
        st.form_submit_button('Search')
    # Most Review Here
    st.header(st.session_state["data"]["most_movie"]["name"])
    st.subheader(st.session_state["data"]["most_movie"]["genre"])
    st.markdown(st.session_state["data"]["most_review"]
                ["content"], unsafe_allow_html=True)
    st.text("--review--"+st.session_state["data"]["most_review"]["topic"])

movies = st.session_state["data"]["movies_recommend"]
if len(movies) > 0:
    with col3:
        n_rows = 1 + len(movies) // int(3)
        rows = [st.container() for _ in range(0, n_rows)]
        cols_per_row = [r.columns(3) for r in rows]
        cols = [column for row in cols_per_row for column in row]

        for image_index, movie in enumerate(movies):
            card = cols[image_index]
            card.image(movie["image"])
            card.subheader(movie["name"])

""" ---------------------------------------------------------------------------------------------------- """
if 'search_dsc' not in st.session_state:
    st.session_state['search_dsc'] = ''

with st.form(key='search_with_dsc'):
    st.text_area("Description", key="search_dsc")
    st.form_submit_button('Search')


search_input_dsc_value = st.session_state["search_dsc"]
if search_input_dsc_value:
    movies_dsc = doc.out_similar(search_input_dsc_value)
    if len(movies_dsc) > 0:
        n_rows = 1 + len(movies_dsc) // int(4)
        rows = [st.container() for _ in range(0, n_rows)]
        cols_per_row = [r.columns(4) for r in rows]
        cols = [column for row in cols_per_row for column in row]

        for image_index, movie in enumerate(movies_dsc):
            card = cols[image_index]
            card.image(movie["image"])
            card.subheader(movie["name"])
