# ===================================================         /   /   E T L   /   /        ================================================== #


# ==================================================     /    IMPORT LIBRARY    /    ======================================================== #

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

import streamlit as st

# =====================================================    /   CLONING   /     ============================================================== #

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


# ===============================================    /    DATA PROCESSING     /   =========================================================== #

#==============================     DATA     /     AGGREGATED     /     TRANSACTION     ===================================#

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
        message_popup('Success','Transformed data loaded to database successfully.')
    except Exception as e:
        print(e)
        message_popup('Error','Error occurred while transforming the data.')

def message_popup(title, message):
    with st.expander(title):
        st.write(message)
        st.button("Close")
# =========================================================================================================================================== #
# Define the pages
def home():
    st.write("Welcome to the Home Page!")
    # Display buttons horizontally using st.columns
    col1, col2,col3,col4,col5,col6,col7,col8,col9 = st.columns(9)
    btn_extract = col1.button("Extract Data")
    btn_transform = col2.button("Load Data")
    if btn_extract:
        with st.spinner("Extracting data..."):
            repo_url ="https://api.github.com/repos/PhonePe/pulse"
            clone_github_repo(repo_url,data_path) 
    if btn_transform:
        with st.spinner("Extracting data..."):
            transform_load_data(data_path)

def process(data_path):
    st.write("Clone data from GitHub")
    col1, col2,col3,col4,col5,col6,col7,col8,col9 = st.columns(9)
    btn_extract = col1.button("Extract Data")
    btn_transform = col2.button("Load Data")
    if btn_extract:
        with st.spinner("Extracting data..."):
            repo_url ="https://api.github.com/repos/PhonePe/pulse"
            clone_github_repo(repo_url,data_path) 
    if btn_transform:
        with st.spinner("Extracting data..."):
            transform_load_data(data_path)

def visualize():
    st.write("Welcome to page!")      

# =========================================================================================================================================== #

def main():
    data_path = 'C:/Users/malar/Documents/github_codewithselva/data-visualization-plotly/data/external/'
    # Set the background color of the entire application
    st.set_page_config(
        page_title="PhonePe Pulse Data Visualization and Exploration",
        page_icon="🌐",  # You can change this to your preferred icon
        layout="wide",
        initial_sidebar_state="expanded",
    )
    #st.title(":blue[PhonePe Pulse Data Visualization and Exploration]")
    #st.markdown("<h1 style='text-align: center; color: blue;'>PhonePe Pulse Data Visualization and Exploration</h1>", unsafe_allow_html=True)
    title_html = """
    <style>
        body {
            background-color: #222222; /* Set your desired background color */
        }
        .title-container {
            background-color: #2D9596; /* Set your desired background color */
            padding: 5px;
            border-radius: 10px;
            text-align: center;
        }
        .title-text {
            color: #ffffff;
        }
        .nav-bar {
            background-color: #9AD0C2;
            padding: 10px;
            border-radius: 10px;
            display: flex;
            justify-content: Left;
        }
        .nav-link {
            color: #265073;
            margin: 0 15px;
            text-decoration: none;
            cursor: pointer;
            font-weight: bold;
            transition: background-color 0.3s;
        }
        .nav-link:hover {
                background-color: #C8F9F6; /* Set your desired background color on hover */
        }
        .active-tab {
            background-color: #C8F9F6; /* Set the background color for the active tab */
            color: #ffffff; /* Set the text color for the active tab */
    }
    </style>
    <script>
        function navigate(page) {
            const contentContainer = document.getElementById('content-container');
            const navLinks = document.querySelectorAll('.nav-link');

            // Remove 'active-tab' class from all tabs
            navLinks.forEach(link => link.classList.remove('active-tab'));

            switch (page) {
                case 1:
                    contentContainer.innerHTML = '';
                    break;
                case 2:
                    contentContainer.innerHTML = '<iframe src="/process" width="100%" height="800px"></iframe>';
                    break;
                case 3:
                    contentContainer.innerHTML = '<iframe src="/visualize" width="100%" height="800px"></iframe>';
                    break;
            }

            // Add 'active-tab' class to the clicked tab
            navLinks[page - 1].classList.add('active-tab');
        }
    </script>
    <div class="title-container">
        <h1 class="title-text">PhonePe Pulse Data Visualization and Exploration</h1>
    </div>
    <div class="nav-bar">
        <div class="nav-link" onclick="navigate(1)">Home</div>
        <div class="nav-link" onclick="navigate(2)">Data Processing</div>
        <div class="nav-link" onclick="navigate(3)">Data Visualization</div>
    </div>
    <div id="content-container"></div>
    
    """
    st.markdown(title_html, unsafe_allow_html=True)
    # Get the current page from the URL
    current_page = st.experimental_get_query_params().get("page", [1])[0]
    print(current_page)

    # Display the content based on the current page
    if current_page == 1:
        home()
    elif current_page == 2:
        process(data_path)
    elif current_page == 3:
        visualize()


if __name__ == "__main__":
    main()    