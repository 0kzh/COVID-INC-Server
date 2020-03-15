import requests
from bs4 import BeautifulSoup
import psycopg2
from time import gmtime, strftime
import time
import unidecode
import pandas as pd
import re
from iso_codes import iso_codes
from populations import populations

class Coronavirus():
    def __init__(self):
        print("initializing")
        self.db = psycopg2.connect(
            database="coronavirus",
            user="***REMOVED***",
            password="***REMOVED***",
            host="***REMOVED***",
            port='***REMOVED***'
        )

    def convertDigit(this, string):
        if string.replace(",", "").isdigit():
            return int(string.replace(",", ""))
        return string

    def strip(this, s):
        if s:
            return re.sub('\D', '', str(s))
        else:
            return 0

    def get_data(this):
        url = 'https://www.worldometers.info/coronavirus/#countries'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser") # Parse html

        table = soup.find("table", {"id": "main_table_countries"}).find_all("tbody") # table
        tr_elems = table[0].find_all("tr") # All rows in table

        data = []
        cursor = this.db.cursor()
        print(dir(cursor))
        today = time.gmtime()
        day = time.strftime('%Y-%m-%d', today)
        
        for tr in tr_elems: # Loop through rows
            td_elems = tr.find_all("td") # Each column in row
            row = [this.convertDigit(td.text.strip()) for td in td_elems]

            country = unidecode.unidecode(row[0])
            iso = iso_codes[country] if country in iso_codes else ""
            population = populations[country] if country in populations else ""
            total_cases = this.strip(row[1])
            new_cases = this.strip(row[2])
            total_deaths = this.strip(row[3])
            new_deaths = this.strip(row[4])
            recovered = this.strip(row[5])
            active = this.strip(row[6])
            serious = this.strip(row[7])

            # all countries but diamond princess
            if iso != "DP":
                data.append((day, country, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population, day, iso))
        
        # mass insert
        # print(data)
        sql = """
            INSERT INTO countries_daily (day, name, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM countries_daily WHERE day = %s AND iso = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()
        this.db.close()



bot = Coronavirus()
bot.get_data()
# bot.create_table()
