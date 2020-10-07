import requests
from bs4 import BeautifulSoup
from bs4 import Tag
import psycopg2
from time import gmtime, strftime
import time
import unidecode
import pandas as pd
import re
from iso_codes import iso_codes
from populations import populations
from selenium import webdriver
from datetime import timedelta, date
import dateparser

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

from newsapi import NewsApiClient

import smtplib
from email.mime.text import MIMEText

def send_email_failed():
    smtp_ssl_host = 'smtp.gmail.com'
    smtp_ssl_port = 465
    username = 'covidinc0@gmail.com'
    password = '***REMOVED***'
    sender = 'covidinc0@gmail.com'
    targets = ['me@kelvinzhang.ca', 'william.qin51@gmail.com']

    text = """
    Scraping failed! Go and fix it :P
    """

    msg = MIMEText(text)
    msg['Subject'] = 'COVID Inc Scraping Failed'
    msg['From'] = sender
    msg['To'] = ', '.join(targets)

    server = smtplib.SMTP_SSL(smtp_ssl_host, smtp_ssl_port)
    server.login(username, password)
    server.sendmail(sender, targets, msg.as_string())
    server.quit()

class Coronavirus():
    def __init__(self):
        print("initializing")
        self.driver = webdriver.Chrome(ChromeDriverManager().install())
        self.db = psycopg2.connect(
            database="coronavirus",
            user="***REMOVED***",
            password="***REMOVED***",
            host="***REMOVED***",
            port='***REMOVED***'
        )
    
    def close_conn(self):
        self.db.close()

    def close_driver(self):
        self.driver.close()

    def convertDigit(this, string):
        if string.replace(",", "").isdigit():
            return int(string.replace(",", ""))
        return string

    def strip(this, s):
        if s:
            return re.sub('\D', '', str(s))
        return 0

    def get_news_2(this):
        url = 'https://coronavirus.thebaselab.com/'
        cursor = this.db.cursor()

        this.driver.get(url)
        delay = 10
        try:
            elem = WebDriverWait(this.driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'jumbotron')))
        except TimeoutException:
            print("Timeout when loading " + url)
        soup = BeautifulSoup(this.driver.page_source, "html.parser")

        news = soup.find_all("div", {"class": "jumbotron"})
        data = []
        for article in news:
            # find closest parent starting with outbreak-tabcontent
            date_elem = article.find("div", {"class": "text-right"}).getText()

            date_string = date_elem.split(u'\u00b7')[0].lstrip().rstrip()
            date = dateparser.parse(date_string) # parse human readable date into datetime obj.
            day = date.strftime('%Y-%m-%d')

            headlineText = article.find("h6").getText()
            headlineText = headlineText.split(u"\u3011")[1]
            desc = str(article.find("h5")).replace("h5", "p")
            print((day, headlineText, desc, day, headlineText))
            data.append((day, headlineText, desc, day, headlineText))
        
        # mass insert
        sql = """
            INSERT INTO news (day, headline, description)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM news WHERE day = %s AND headline = %s);
            """
        cursor.executemany(sql, tuple(data))
        this.db.commit()
        this.driver.close()
        cursor.close()

    def get_news(this, date):
        cursor = this.db.cursor() 
        newsapi = NewsApiClient(api_key='***REMOVED***')

        queries = ['covid-19 breaking news global']
        fulldata = pd.DataFrame()
        for q in queries:
            json_data = newsapi.get_everything(q=q, language='en', from_param=date, to=date)
            data = pd.DataFrame(json_data['articles'])

            if len(data)>0:
                data['source'] = data['source'].apply(lambda x : x['name'])
                data['publishedAt'] = pd.to_datetime(data['publishedAt'])
                fulldata = pd.concat([fulldata,data])

        # only get 5 top headlines for that day
        fulldata = fulldata.head(5)

        if len(fulldata) > 0:
            fulldata = fulldata.drop_duplicates(subset='url').sort_values(by='publishedAt', ascending=False).reset_index()

        subset = fulldata[['publishedAt', 'title', 'description', 'publishedAt', 'title']]
        data = [tuple(x) for x in subset.to_numpy()]

        # day, headlineText, desc, day, headlineText  
        # mass insert
        sql = """
            INSERT INTO news (day, headline, description)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM news WHERE day = %s AND headline = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()


    def get_data(this, date):
        def set_default_int(x):
            return x if x else -1

        print(f"Fetching data for {date}")
        print("--------------------------------")
        url = f"https://web.archive.org/web/{date}010005/https://www.worldometers.info/coronavirus/"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser") # Parse html

        # id = table3 if 
        #    =        if
        table = soup.find("table", {"id": "main_table_countries_today"})
        cols = table.find("thead").find_all("th")

        col_mapping = dict()
        for i, col in enumerate(cols):
            header = col.get_text()
            col_mapping[header] = i

        print(col_mapping)

        tbody = table.find_all("tbody") # table
        tr_elems = tbody[0].find_all("tr") # All rows in table

        data = []
        cursor = this.db.cursor()

        date_elem = soup.find_all(lambda tag:tag.name=="div" and "Last updated:" in tag.text)[-1].getText()
        date_string = date_elem.replace("Last updated: ", "")
        date = dateparser.parse(date_string) # parse human readable date into datetime obj.
        day = date.strftime('%Y-%m-%d')
        
        # get world cases
        numbers = soup.find_all("div", {"class": "maincounter-number"})
        world_cases = this.strip(numbers[0].getText())
        world_deaths = this.strip(numbers[1].getText())
        world_recovered = this.strip(numbers[2].getText()) if len(numbers) >= 3 else "-1"

        active = soup.find("div", {"class": "number-table-main"})
        world_active = this.strip(active.getText()) if active is not None else "-1"

        serious = soup.find_all("span", {"class": "number-table"})
        world_serious = this.strip(serious[1].getText()) if len(serious) >= 1 else "-1"

        data.append((day, "World", "WR", world_cases, "-1", world_deaths, "-1", world_recovered, world_active, world_serious, "-1", day, ""))

        unmapped_countries = []
        for tr in tr_elems: # Loop through rows
            td_elems = tr.find_all("td") # Each column in row
            row = [this.convertDigit(td.text.strip()) for td in td_elems]

            country = unidecode.unidecode(row[col_mapping['Country,Other']])
            if not country in iso_codes and not country in populations:
                unmapped_countries.append(country)

            iso = iso_codes[country] if country in iso_codes else ""
            population = populations[country] if country in populations else "-1"

            total_cases = set_default_int(this.strip(row[col_mapping['TotalCases']]))
            new_cases = set_default_int(this.strip(row[col_mapping['NewCases']]))
            total_deaths = set_default_int(this.strip(row[col_mapping['TotalDeaths']]))
            new_deaths = set_default_int(this.strip(row[col_mapping['NewDeaths']]))
            recovered = set_default_int(this.strip(row[col_mapping['TotalRecovered']]))
            active = set_default_int(this.strip(row[col_mapping['ActiveCases']]))
            serious = set_default_int(this.strip(row[col_mapping['Serious,Critical']]))

            # all countries but diamond princess
            if iso != "DP" and iso != "MSZ":
                data.append((day, country, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population, day, iso))
        
        # mass insert
        for row in range(10):
            print(data[row])

        print(f"Unmapped countries: {unmapped_countries}")
        sql = """
            INSERT INTO countries_daily (day, name, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM countries_daily WHERE day = %s AND iso = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()

        print("Completed")
        print("--------------------------------")

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


bot = Coronavirus()

HISTORY_MODE = False

try:
    if HISTORY_MODE:
        start_date = date(2020, 8, 7)
        end_date = date(2020, 9, 6)
        for single_date in daterange(start_date, end_date):
            formatted_date = single_date.strftime("%Y%m%d")
            news_date = single_date.strftime("%Y-%m-%d")
            bot.get_news(news_date)
            #bot.get_data(formatted_date)
        bot.close_conn()
    else:
        today = date.today()
        formatted_date = today.strftime("%Y%m%d")
        news_date = today.strftime("%Y-%m-%d")
        bot.get_news(news_date)
        bot.get_data(formatted_date)

except Exception as e:
    raise e

bot.close_driver()