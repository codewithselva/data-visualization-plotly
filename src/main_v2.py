import streamlit as st
# [clone libraries]
import requests
import subprocess
from subprocess import CalledProcessError
# import git

# [pandas and file handling libraries]
import pandas as pd
import os
import json

# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

import pydeck as pdk
import json


def clone_github_repo(repo,data_url):
    try:
        #Specify the GitHub repository URL
        response = requests.get(repo)
        repo = response.json()
        clone_url = repo['clone_url']
        #Specify the local directory path
        clone_dir = data_url
        # Clone the repository to the specified local directory
        subprocess.run(["git", "clone", clone_url, clone_dir], check=True)
        message_popup('Success!','Cloning completed.')
    except CalledProcessError as cpe:
        print(cpe)
        message_popup('Warning!','Process exception occurred while cloning the repository.')
    except Exception as e:
        print(e)
        message_popup('Error!','Error occurred while cloning the repository.')

def transform_load_data(data_path):
    try:
        # 1
        path_1 = data_path+"data/aggregated/transaction/country/india/state/"
        Agg_tran_state_list = os.listdir(path_1)

        Agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

        for i in Agg_tran_state_list:
            p_i = path_1 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    A = json.load(Data)
                    
                    for l in A['data']['transactionData']:
                        Name = l['name']
                        count = l['paymentInstruments'][0]['count']
                        amount = l['paymentInstruments'][0]['amount']
                        Agg_tra['State'].append(i)
                        Agg_tra['Year'].append(j)
                        Agg_tra['Quarter'].append(int(k.strip('.json')))
                        Agg_tra['Transaction_type'].append(Name)
                        Agg_tra['Transaction_count'].append(count)
                        Agg_tra['Transaction_amount'].append(amount)
                        
        df_aggregated_transaction = pd.DataFrame(Agg_tra)

        #==============================     DATA     /     AGGREGATED     /     USER     ===================================#
        # 2

        path_2 = data_path+"data/aggregated/user/country/india/state/"
        Agg_user_state_list = os.listdir(path_2)

        Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

        for i in Agg_user_state_list:
            p_i = path_2 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    B = json.load(Data)
                    
                    try:
                        for l in B["data"]["usersByDevice"]:
                            brand_name = l["brand"]
                            count_ = l["count"]
                            ALL_percentage = l["percentage"]
                            Agg_user["State"].append(i)
                            Agg_user["Year"].append(j)
                            Agg_user["Quarter"].append(int(k.strip('.json')))
                            Agg_user["Brands"].append(brand_name)
                            Agg_user["User_Count"].append(count_)
                            Agg_user["User_Percentage"].append(ALL_percentage*100)
                    except:
                        pass

        df_aggregated_user = pd.DataFrame(Agg_user)

        #==============================     DATA     /     MAP     /     TRANSACTION     =========================================#
        # 3

        path_3 = data_path+"data/map/transaction/hover/country/india/state/"
        map_tra_state_list = os.listdir(path_3)

        map_tra = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}

        for i in map_tra_state_list:
            p_i = path_3 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    C = json.load(Data)
                    
                    for l in C["data"]["hoverDataList"]:
                        District = l["name"]
                        count = l["metric"][0]["count"]
                        amount = l["metric"][0]["amount"]
                        map_tra['State'].append(i)
                        map_tra['Year'].append(j)
                        map_tra['Quarter'].append(int(k.strip('.json')))
                        map_tra["District"].append(District)
                        map_tra["Transaction_Count"].append(count)
                        map_tra["Transaction_Amount"].append(amount)
                        
        df_map_transaction = pd.DataFrame(map_tra)

        #==============================         DATA     /     MAP     /     USER         ============================================#
        # 4

        path_4 = data_path+"data/map/user/hover/country/india/state/"
        map_user_state_list = os.listdir(path_4)

        map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

        for i in map_user_state_list:
            p_i = path_4 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)

                    for l in D["data"]["hoverData"].items():
                        district = l[0]
                        registereduser = l[1]["registeredUsers"]
                        map_user['State'].append(i)
                        map_user['Year'].append(j)
                        map_user['Quarter'].append(int(k.strip('.json')))
                        map_user["District"].append(district)
                        map_user["Registered_User"].append(registereduser)
                        
        df_map_user = pd.DataFrame(map_user)

        #==============================     DATA     /     TOP     /     TRANSACTION     =========================================#
        # 5

        path_5 = data_path+"data/top/transaction/country/india/state/"
        top_tra_state_list = os.listdir(path_5)

        top_tra = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

        for i in top_tra_state_list:
            p_i = path_5 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    E = json.load(Data)
                    
                    for l in E['data']['pincodes']:
                        Name = l['entityName']
                        count = l['metric']['count']
                        amount = l['metric']['amount']
                        top_tra['State'].append(i)
                        top_tra['Year'].append(j)
                        top_tra['Quarter'].append(int(k.strip('.json')))
                        top_tra['District_Pincode'].append(Name)
                        top_tra['Transaction_count'].append(count)
                        top_tra['Transaction_amount'].append(amount)

        df_top_transaction = pd.DataFrame(top_tra)

        #==============================     DATA     /     TOP     /     USER     ============================================#
        # 6

        path_6 = data_path+"data/top/user/country/india/state/"
        top_user_state_list = os.listdir(path_6)

        top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

        for i in top_user_state_list:
            p_i = path_6 + i + "/"
            Agg_yr = os.listdir(p_i)

            for j in Agg_yr:
                p_j = p_i + j + "/"
                Agg_yr_list = os.listdir(p_j)

                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    F = json.load(Data)
                    
                    for l in F['data']['pincodes']:
                        Name = l['name']
                        registeredUser = l['registeredUsers']
                        top_user['State'].append(i)
                        top_user['Year'].append(j)
                        top_user['Quarter'].append(int(k.strip('.json')))
                        top_user['District_Pincode'].append(Name)
                        top_user['Registered_User'].append(registeredUser)
                        
        df_top_user = pd.DataFrame(top_user)

        #==============================     DATA     /     MAP     /     COORDINATES     =========================================#
        # 7

        path_7 = data_path+"data/map/insurance/country/india/"
        year_list = os.listdir(path_7)

        map_coordinates = {'State': [], 'Latitude': [], 'Longitude': [], 'Metric': []}

        for year in year_list:
            #print(year)
            if(year.isdigit()):
                path_year = path_7 + year + "/"
                file_list = os.listdir(path_year)
                #print("file_list : ",file_list)
                for file in file_list:
                    path_file = path_year + file
                    data_file = open(path_file, 'r')
                    data_file_json = json.load(data_file)
                    #print("data_file : ",data_file_json['data']['data']['data'])
                    for data in data_file_json['data']['data']['data']:
                        latitude = data[0]
                        longitude = data[1]
                        metric =data[2]
                        state = data[3]
                        map_coordinates['Latitude'].append(latitude)
                        map_coordinates['Longitude'].append(longitude)
                        map_coordinates['Metric'].append(metric)
                        map_coordinates['State'].append(state)
        df_map_coordinates = pd.DataFrame(map_coordinates)




        #  =============     CONNECT SQL SERVER  /   CREAT DATA BASE    /  CREAT TABLE    /    STORE DATA    ========  #

        # Connect to the MySQL server
        mydb = mysql.connector.connect(
                                        host = "localhost",
                                        user = "root",
                                        password = "password",
                                        auth_plugin = "mysql_native_password"
                                        )

        # Create a new database and use
        mycursor = mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")

        # Close the cursor and database connection
        mycursor.close()
        mydb.close()

        # Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:password@localhost/phonepe_pulse', echo=False)

        # Use pandas to insert the DataFrames datas to the SQL Database -> table1

        # 1
        df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,   
                                        dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                            'Year': sqlalchemy.types.Integer, 
                                            'Quater': sqlalchemy.types.Integer, 
                                            'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                            'Transaction_count': sqlalchemy.types.Integer,
                                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
        # 2
        df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,
                                dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                        'Year': sqlalchemy.types.Integer, 
                                        'Quater': sqlalchemy.types.Integer,
                                        'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                        'User_Count': sqlalchemy.types.Integer, 
                                        'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
        # 3                       
        df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False,
                                dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                        'Year': sqlalchemy.types.Integer, 
                                        'Quater': sqlalchemy.types.Integer, 
                                        'District': sqlalchemy.types.VARCHAR(length=50), 
                                        'Transaction_Count': sqlalchemy.types.Integer, 
                                        'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
        # 4
        df_map_user.to_sql('map_user', engine, if_exists = 'replace', index=False,
                        dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer, 
                                'District': sqlalchemy.types.VARCHAR(length=50), 
                                'Registered_User': sqlalchemy.types.Integer, })
        # 5                  
        df_top_transaction.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                                dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                        'Year': sqlalchemy.types.Integer, 
                                        'Quater': sqlalchemy.types.Integer,   
                                        'District_Pincode': sqlalchemy.types.Integer,
                                        'Transaction_count': sqlalchemy.types.Integer, 
                                        'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
        # 6
        df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False,
                        dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,                           
                                'District_Pincode': sqlalchemy.types.Integer, 
                                'Registered_User': sqlalchemy.types.Integer,})

        #7
        df_map_coordinates.to_sql('state_coordinates',engine,if_exists='replace', index=False,
        dtype={
            'State':sqlalchemy.types.VARCHAR(length=50), 
            'Latitude': sqlalchemy.types.DECIMAL(precision=18, scale=15, asdecimal=True),
            'Longitude': sqlalchemy.types.DECIMAL(precision=18, scale=15, asdecimal=True),
            'Metric': sqlalchemy.types.FLOAT(precision=5, asdecimal=True),
        })
        message_popup('Success','Transformed data loaded to database successfully.')
    except Exception as e:
        print(e)
        message_popup('Error','Error occurred while transforming the data.')

def visualize():
    with open(r'C:/Users/malar/Downloads/states_india.geojson') as file:
        india_geo = json.load(file)

    # Extract the view parameters for India
    view = pdk.data_utils.compute_view(india_geo["features"][0]["geometry"]["coordinates"][0][0])

    # Create a GeoJSONLayer to display the map of India
    layer = pdk.Layer(
        #"GeoJsonLayer",
        "HexagonLayer",
        data=india_geo,
        get_fill_color=[238, 245, 126, 160],  # Adjust the color as needed
        get_line_color=[5, 28, 171],
        elevation_scale=500,
        pickable=True,
    )

    # Create a HexagonLayer with extruded bars
    hexagon_layer = pdk.Layer(
        "HexagonLayer",
        data=india_geo,
        get_fill_color=[238, 245, 126, 160],  # Adjust the color as needed
        get_line_color=[5, 28, 171],
        elevation_scale=500,
        extruded=True,
        pickable=True,
        coverage=1,
        auto_highlight=True,
        radius=10000,
        upper_percentile=100,
        material=True,
        elevation_range=[0, 1000],
        elevation_aggregation="SUM",
    )

    # Create a PyDeck deck
    deck = pdk.Deck(
        layers=[layer,hexagon_layer],
        initial_view_state=view,
        tooltip={"text": "{elevationValue}"}
    )

    # Show the PyDeck deck
    #deck.to_html("india_map.html")
    st.pydeck_chart(deck)

def message_popup(title, message):
    with st.expander(title):
        st.write(message)
        st.button("Close")

# Define pages
def home():
    st.write("Welcome to the Home Page!")

def data_processing(repo_url,data_path):
    st.write("Welcome to the Data Processing Page!")
    col1, col2,col3,col4,col5,col6 = st.columns(6)
    btn_extract = col1.button("Extract Data")
    btn_transform = col2.button("Load Data")
    if btn_extract:
        with st.spinner("Extracting data..."):
            repo_url ="https://api.github.com/repos/PhonePe/pulse"
            clone_github_repo(repo_url,data_path) 
    if btn_transform:
        with st.spinner("Extracting data..."):
            transform_load_data(data_path)

def data_visualization():
    st.write("Welcome to the Data Visualization Page!")
    visualize()

def about():
    st.write("Welcome to the About Page!")

# Define the main function
def main():
    # Set the background color of the entire application
    st.set_page_config(
        page_title="PhonePe Pulse Data Visualization and Exploration",
        page_icon="🌐",  # You can change this to your preferred icon
        layout="wide",
        initial_sidebar_state="expanded",
    )
    data_path = 'C:/Users/malar/Documents/github_codewithselva/data-visualization-plotly/data/external/'
    repo_url ="https://api.github.com/repos/PhonePe/pulse"
    
    st.header("My Streamlit App")
    st.title("Left Navigation Example")

    # Create a sidebar for navigation
    menu = ["Home", "Data Processing", "Data Visualization", "About"]
    choice = st.sidebar.selectbox("Select a Page", menu)

    # Display content based on the selected page
    if choice == "Home":
        home()
    elif choice == "Data Processing":
        data_processing(repo_url,data_path)
    elif choice == "Data Visualization":
        data_visualization()
    elif choice == "About":
        about()
    st.markdown(
        """
        ---
        *Your footer content goes here.*
        """
    )
if __name__ == "__main__":
    main()
