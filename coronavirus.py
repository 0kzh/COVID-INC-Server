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
from selenium import webdriver
import dateparser

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

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
        return 0

    def get_news(this):
        url = 'https://www.pharmaceutical-technology.com/news/coronavirus-a-timeline-of-how-the-deadly-outbreak-evolved'
        cursor = this.db.cursor()

        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        news = soup.find_all("blockquote", {"class": "cc-blockquote"})

        data = []
        for article in news:
            try:
                # find closest parent starting with outbreak-tabcontent
                date_elem = article.find("p", {"class": "update-date"}).getText()
                date_string = date_elem.split(':')[0]
                date = dateparser.parse(date_string + " 2020") # parse human readable date into datetime obj.
                day = date.strftime('%Y-%m-%d')

                headlines = article.find_all("h2")
                for headline in headlines:
                    headlineText = headline.getText().rstrip().lstrip()
                    data.append((day, headlineText, day, headlineText))
            except AttributeError:
                continue
        
        # mass insert
        sql = """
            INSERT INTO news (day, headline)
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT id FROM news WHERE day = %s AND headline = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()
        this.db.close()


    def get_data(this):
        url = 'https://www.worldometers.info/coronavirus/#countries'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser") # Parse html

        table = soup.find("table", {"id": "main_table_countries"}).find_all("tbody") # table
        tr_elems = table[0].find_all("tr") # All rows in table

        data = []
        cursor = this.db.cursor()
        today = time.gmtime()
        day = time.strftime('%Y-%m-%d', today)
        
        # get world cases
        numbers = soup.find_all("div", {"class": "maincounter-number"})
        world_cases = this.strip(numbers[0].getText())
        world_deaths = this.strip(numbers[1].getText())
        world_recovered = this.strip(numbers[2].getText())

        active = soup.find("div", {"class": "number-table-main"})
        world_active = this.strip(active.getText())

        serious = soup.find_all("span", {"class": "number-table"})[1]
        world_serious = this.strip(serious.getText())

        data.append((day, "World", "", world_cases, "-1", world_deaths, "-1", world_recovered, world_active, world_serious, "-1", day, ""))

        for tr in tr_elems: # Loop through rows
            td_elems = tr.find_all("td") # Each column in row
            row = [this.convertDigit(td.text.strip()) for td in td_elems]

            country = unidecode.unidecode(row[0])
            iso = iso_codes[country] if country in iso_codes else ""
            population = populations[country] if country in populations else "-1"
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
# bot.get_data()
bot.get_news()
# bot.create_table()
