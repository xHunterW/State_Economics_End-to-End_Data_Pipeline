from flask import Flask, request, jsonify
import requests
import sqlite3
import pandas as pd
import json 
import psycopg2
from bs4 import BeautifulSoup


app = Flask(__name__)



saved_data = {}

# Define a POST endpoint to retrieve all data from the database
@app.route('/get_data', methods=['POST'])
def get_data():
    response = request.get_json()
    test = json.loads(response)
    table_name = str(test['table'])
    limit = str(test['limit'])

    query = "SELECT * FROM final_project." + table_name  + " LIMIT " + limit

    #Connecting with online database
    conn = psycopg2.connect('postgres://avnadmin:AVNS_dUt8uqSbJlEOdm6DOlb@project-jh-e494.d.aivencloud.com:22077/defaultdb?sslmode=require')

    ## Create a cursor object to interface with fetching data from postgres database
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]


        
    return "Here are the column names: " + str(colnames) + ". and " + "here are the " + limit + " rows chosen: " + str(rows) + "."



if __name__ == '__main__': 
    app.run(debug=True, port=80) 






