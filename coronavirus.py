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
import dateparser

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

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
    Hello,
    Thank you for submitting your application and for your interest in Covid Inc, specifically the Software Engineering Intern position. We have received tens of thousands of applications for our summer internships.
    While your skills and background are impressive, after assessing all of the candidates that applied for this position, we have decided to proceed with other applicants who more closely fit our needs at this time.
    
    Again, we really appreciate all of the time and effort that you took to go through the process with us and we wish you success in your search for an internship.
    
    Sincerely,
    Covid Inc
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
        self.driver = webdriver.Chrome()
        self.db = psycopg2.connect(
            database="coronavirus",
            user="***REMOVED***",
            password="***REMOVED***",
            host="***REMOVED***",
            port='***REMOVED***'
        )
    
    def close_conn(self):
        self.db.close()

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
        cursor.executemany(sql, tuple(data));
        this.db.commit()
        this.driver.close()
        cursor.close()

    def get_news(this):
        url = 'https://www.pharmaceutical-technology.com/news/coronavirus-timeline/'
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
                    nextNode = headline
                    headlineText = headline.getText().rstrip().lstrip()


                    # concat html of description tags
                    desc = ""

                    while True:
                        nextNode = nextNode.find_next_sibling("")
                        try:
                            tag_name = nextNode.name
                        except AttributeError:
                            tag_name = ""
                        if tag_name == "p":
                            desc += str(nextNode)
                        else:
                            break
                    day = "2019-12-31" if headlineText == "First cases detected" else day
                    data.append((day, headlineText, desc, day, headlineText))
            except AttributeError:
                continue
        
        # mass insert
        sql = """
            INSERT INTO news (day, headline, description)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM news WHERE day = %s AND headline = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()


    def get_data(this):
        url = 'https://www.worldometers.info/coronavirus/'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser") # Parse html

        # id = table3 if 
        #    =        if
        table = soup.find("table", {"id": "main_table_countries_today"}).find_all("tbody") # table
        # print(table)
        tr_elems = table[0].find_all("tr") # All rows in table

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

        for tr in tr_elems: # Loop through rows
            td_elems = tr.find_all("td") # Each column in row
            row = [this.convertDigit(td.text.strip()) for td in td_elems]

            country = unidecode.unidecode(row[0])
            iso = iso_codes[country] if country in iso_codes else ""
            population = populations[country] if country in populations else "-1"

            if len(row) >= 8:
                total_cases = this.strip(row[1])
                new_cases = this.strip(row[2])
                total_deaths = this.strip(row[3])
                new_deaths = this.strip(row[4])
                recovered = this.strip(row[5])
                active = this.strip(row[6])
                serious = this.strip(row[7])
            elif len(row) == 6:
                total_cases = this.strip(row[1])
                new_cases = this.strip(row[2])
                total_deaths = this.strip(row[3])
                new_deaths = this.strip(row[4])
                recovered = "-1"
                active = "-1"
                serious = "-1"
            elif len(row) == 7:
                total_cases = this.strip(row[1])
                new_cases = this.strip(row[2])
                total_deaths = this.strip(row[3])
                new_deaths = this.strip(row[4])
                recovered = this.strip(row[5])
                active = "-1"
                serious = this.strip(row[6])

            # all countries but diamond princess
            if iso != "DP":
                data.append((day, country, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population, day, iso))
        
        # mass insert
        print(data)
        sql = """
            INSERT INTO countries_daily (day, name, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious, population)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT id FROM countries_daily WHERE day = %s AND iso = %s);
            """
        cursor.executemany(sql, tuple(data));
        this.db.commit()

        cursor.close()

bot = Coronavirus()
try:
    bot.get_news()
    bot.get_news_2()
    bot.get_data()
    bot.close_conn()
except:
    send_email_failed()