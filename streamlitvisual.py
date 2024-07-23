

import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
from pathlib import Path

st.set_page_config(page_title="Country Data Analysis", layout="wide")


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

# load_dotenv()

DATABASE_URI = os.getenv('DATABASE_URI')

def run_queries(database_uri, queries):
    conn = psycopg2.connect(database_uri)
    cur = conn.cursor()

    results = {}
    for description, query in queries.items():
        cur.execute(query)
        result = cur.fetchall()
        results[description] = result

    cur.close()
    conn.close()
    return results


main_queries = {
    "Countries that speak French": "SELECT 'Countries that speak French' AS category, COUNT(*) AS count FROM countries WHERE languages LIKE '%French%';",
    "Countries that speak English": "SELECT 'Countries that speak English' AS category, COUNT(*) AS count FROM countries WHERE languages LIKE '%English%';",
    "Countries with more than 1 official language": "SELECT 'Countries with more than 1 official language' AS category, COUNT(*) AS count FROM countries WHERE array_length(string_to_array(languages, ', '), 1) > 1;",
    "Countries with Euro as official currency": "SELECT 'Countries with Euro as official currency' AS category, COUNT(*) AS count FROM countries WHERE currency_name = 'Euro';",
    "Countries from Western Europe": "SELECT 'Countries from Western Europe' AS category, COUNT(*) AS count FROM countries WHERE sub_region = 'Western Europe';",
    "Countries not yet independent": "SELECT 'Countries not yet independent' AS category, COUNT(*) AS count FROM countries WHERE independence = FALSE;",
    "Distinct continents and number of countries from each": "SELECT continents AS category, COUNT(*) AS count FROM countries GROUP BY continents;",
    "Countries whose start of the week is not Monday": "SELECT 'Countries whose start of the week is not Monday' AS category, COUNT(*) AS count FROM countries WHERE start_of_week != 'monday';",
    "Countries not a United Nation member": "SELECT 'Countries not a United Nation member' AS category, COUNT(*) AS count FROM countries WHERE un_member = FALSE;",
    "Countries that are United Nation members": "SELECT 'Countries that are United Nation members' AS category, COUNT(*) AS count FROM countries WHERE un_member = TRUE;",
    "Least 2 countries with the lowest population for each continent": """
        SELECT country_name, continents AS category, population
        FROM (
            SELECT country_name, continents, population, 
                   ROW_NUMBER() OVER (PARTITION BY continents ORDER BY population ASC) as rn
            FROM countries
        ) as subquery
        WHERE rn <= 2;
    """,
    "Top 2 countries with the largest area for each continent": """
        SELECT country_name, continents AS category, area
        FROM (
            SELECT country_name, continents, area, 
                   ROW_NUMBER() OVER (PARTITION BY continents ORDER BY area DESC) as rn
            FROM countries
        ) as subquery
        WHERE rn <= 2;
    """,
    "Top 5 countries with the largest area": "SELECT country_name AS category, area FROM countries ORDER BY area DESC LIMIT 5;",
    "Top 5 countries with the lowest area": "SELECT country_name AS category, area FROM countries ORDER BY area ASC LIMIT 5;"
}

# other insight
insight_queries = {
    "Average Population and Area by Continent": """
        SELECT "continents" AS category, AVG("population") AS avg_population, AVG("area") AS avg_area
        FROM countries
        GROUP BY "continents";
    """,
    "Number of Countries by Region": """
        SELECT "region" AS category, COUNT(*) AS number_of_countries
        FROM countries
        GROUP BY "region";
    """,
    "Number of Countries by Currency": """
        SELECT "currency_name" AS category, COUNT(*) AS number_of_countries
        FROM countries
        GROUP BY "currency_name"
        ORDER BY number_of_countries DESC;
    """,
    "Number of Countries by Language": """
        SELECT unnest(string_to_array("languages", ', ')) AS category, COUNT(*) AS number_of_countries
        FROM countries
        GROUP BY category
        ORDER BY number_of_countries DESC;
    """,
    "Top 10 Densest Countries": """
        SELECT "country_name" AS category, "population", "area", ("population" / "area") AS density
        FROM countries
        ORDER BY density DESC
        LIMIT 10;
    """,
    "Number of Languages by Continent": """
        SELECT c."continents" AS category, COUNT(DISTINCT lang.language) AS number_of_languages
        FROM countries c,
        LATERAL unnest(string_to_array(c."languages", ', ')) AS lang(language)
        GROUP BY c."continents"
        ORDER BY number_of_languages DESC
        LIMIT 8;
    """
}


st.title("Country Data Analysis")

page = st.sidebar.selectbox("Choose a page", ["Main Analysis", "Other Insights"])

if page == "Main Analysis":
    try:
        engine = create_engine(DATABASE_URI)
        conn = engine.connect()
        df = pd.read_sql("SELECT * FROM countries", conn)
        st.header("Country Data")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error loading data: {e}")

    query_results = run_queries(DATABASE_URI, main_queries)
    for description, result in query_results.items():
        st.header(description)
        if description in ["Distinct continents and number of countries from each", 
                           "Countries that speak French", 
                           "Countries that speak English",
                           "Countries with more than 1 official language",
                           "Countries with Euro as official currency",
                           "Countries from Western Europe",
                           "Countries not yet independent",
                           "Countries whose start of the week is not Monday",
                           "Countries not a United Nation member",
                           "Countries that are United Nation members"]:
            df = pd.DataFrame(result, columns=["category", "count"])
        elif description in ["Least 2 countries with the lowest population for each continent",
                             "Top 2 countries with the largest area for each continent"]:
            df = pd.DataFrame(result, columns=["country_name", "category", "value"])
        elif description in ["Top 5 countries with the largest area", "Top 5 countries with the lowest area"]:
            df = pd.DataFrame(result, columns=["country_name", "value"])
        else:
            df = pd.DataFrame(result)

        st.write(df)

        # Create bar charts
        if 'count' in df.columns:
            st.bar_chart(df.set_index('category')['count'])
        elif 'value' in df.columns:
            st.bar_chart(df.set_index('country_name')['value'])

elif page == "Other Insights":
    query_results = run_queries(DATABASE_URI, insight_queries)
    for description, result in query_results.items():
        st.header(description)
        if description == "Average Population and Area by Continent":
            df = pd.DataFrame(result, columns=["category", "avg_population", "avg_area"])
        elif description == "Number of Countries by Region":
            df = pd.DataFrame(result, columns=["category", "number_of_countries"])
        elif description == "Number of Countries by Currency":
            df = pd.DataFrame(result, columns=["category", "number_of_countries"])
        elif description == "Number of Countries by Language":
            df = pd.DataFrame(result, columns=["category", "number_of_countries"])
        elif description == "Top 10 Densest Countries":
            df = pd.DataFrame(result, columns=["category", "Population", "Area", "density"])
        elif description == "Number of Languages by Continent":
            df = pd.DataFrame(result, columns=["category", "number_of_languages"])
        
        st.write(df)

        # Create bar charts
        if 'number_of_countries' in df.columns:
            st.bar_chart(df.set_index('category')['number_of_countries'])
        elif 'avg_population' in df.columns:
            st.bar_chart(df.set_index('category')[['avg_population', 'avg_area']])
        elif 'density' in df.columns:
            st.bar_chart(df.set_index('category')['density'])
        elif 'number_of_languages' in df.columns:
            st.bar_chart(df.set_index('category')['number_of_languages'])


# Custom CSS for footer
st.markdown("""
    <style>
        footer {visibility: hidden;}
        .footer {
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background-color: #f1f1f1;
            color: black;
            text-align: center;
        }
    </style>
    <div class="footer">
        <p>@Developed by DataAvatars</p>
    </div>
""", unsafe_allow_html=True)
