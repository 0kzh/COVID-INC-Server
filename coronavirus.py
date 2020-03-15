import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import date
import unidecode
import re

iso_codes = {
    "China": "CN",
    "Italy": "IT",
    "Iran": "IR",
    "S. Korea": "KR",
    "Spain": "ES",
    "Germany": "DE",
    "France": "FR",
    "USA": "US",
    "Switzerland": "CH",
    "UK": "GB",
    "Norway": "NO",
    "Sweden": "SE",
    "Netherlands": "NL",
    "Denmark": "DK",
    "Japan": "JP",
    "Diamond Princess": "",
    "Belgium": "BE",
    "Austria": "AT",
    "Qatar": "QA",
    "Australia": "AU",
    "Canada": "CA",
    "Malaysia": "MY",
    "Greece": "GR",
    "Finland": "FI",
    "Bahrain": "BH",
    "Singapore": "SG",
    "Israel": "IL",
    "Czechia": "CZ",
    "Slovenia": "SI",
    "Portugal": "PT",
    "Iceland": "IS",
    "Brazil": "BR",
    "Hong Kong": "HK",
    "Ireland": "IE",
    "Romania": "RO",
    "Estonia": "EE",
    "Philippines": "PH",
    "Iraq": "IQ",
    "Egypt": "EG",
    "Kuwait": "KW",
    "Poland": "PL",
    "Saudi Arabia": "SA",
    "San Marino": "SM",
    "India": "IN",
    "Indonesia": "ID",
    "Lebanon": "LB",
    "UAE": "AE",
    "Thailand": "TH",
    "Chile": "CL",
    "Russia": "RS",
    "Taiwan": "TW",
    "Vietnam": "VN",
    "Luxembourg": "LU",
    "Serbia": "RS",
    "Argentina": "AR",
    "Slovakia": "SK",
    "Bulgaria": "BG",
    "Brunei": "BN",
    "Croatia": "HR",
    "Albania": "AL",
    "Peru": "PE",
    "South Africa": "ZA",
    "Palestine": "PS",
    "Algeria": "DZ",
    "Panama": "PA",
    "Pakistan": "PK",
    "Georgia": "GE",
    "Hungary": "HU",
    "Ecuador": "EC",
    "Belarus": "BY",
    "Costa Rica": "CR",
    "Latvia": "LV",
    "Mexico": "MX",
    "Cyprus": "CY",
    "Colombia": "CO",
    "Senegal": "SN",
    "Bosnia and Herzegovina": "BA",
    "Oman": "OM",
    "Morocco": "MA",
    "Armenia": "AM",
    "Tunisia": "TN",
    "Malta": "MT",
    "Azerbaijan": "AZ",
    "North Macedonia": "MK",
    "Moldova": "MD",
    "Afghanistan": "AF",
    "Dominican Republic": "DO",
    "Macao": "MO",
    "Sri Lanka": "LK",
    "Bolivia": "BO",
    "Maldives": "MV",
    "Martinique": "MQ",
    "Lithuania": "LT",
    "Faeroe Islands": "FO",
    "Jamaica": "JM",
    "Cambodia": "KH",
    "New Zealand": "NZ",
    "French Guiana": "GF",
    "Paraguay": "PY",
    "Kazakhstan": "KZ",
    "Reunion": "RE",
    "Turkey": "TR",
    "Bangladesh": "BD",
    "Cuba": "CU",
    "Liechtenstein": "LI",
    "Uruguay": "UY",
    "Ukraine": "UA",
    "Channel Islands": "KY",
    "French Polynesia": "PF",
    "Guadeloupe": "GP",
    "Honduras": "HN",
    "Puerto Rico": "PR",
    "Monaco": "MC",
    "Nigeria": "NG",
    "Aruba": "AW",
    "Burkina Faso": "BF",
    "Cameroon": "CM",
    "Ivory Coast": "CI",
    "Curacao": "CW",
    "DRC": "CD",
    "Ghana": "GH",
    "Namibia": "NA",
    "Saint Martin": "MF",
    "Seychelles": "SC",
    "Trinidad and Tobago": "TT",
    "Venezuela": "VE",
    "Guyana": "GY",
    "Sudan": "SD",
    "Andorra": "AD",
    "Jordan": "JO",
    "Nepal": "NP",
    "Antigua and Barbuda": "AG",
    "Bhutan": "BT",
    "Cayman Islands": "KY",
    "Ethiopia": "ET",
    "Gabon": "GA",
    "Gibraltar": "GI",
    "Guatemala": "GT",
    "Guinea": "GN",
    "Vatican City": "VA",
    "Kenya": "KE",
    "Mauritania": "MR",
    "Mayotte": "YT",
    "Mongolia": "MN",
    "Rwanda": "RW",
    "St. Barth": "BL",
    "Saint Lucia": "LC",
    "St. Vincent Grenadines": "VC",
    "Suriname": "SR",
    "Eswatini": "SZ",
    "Togo": "TG",
    "U.S. Virgin Islands": "VI",
}

class Coronavirus():
    def __init__(self):
        print("initializing")
        self.db = psycopg2.connect(
            database="coronavirus",
            user="",
            password="",
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
        print(cursor)
        today = date.today()
        day = today.strftime('%Y-%m-%d')
        for tr in tr_elems: # Loop through rows
            td_elems = tr.find_all("td") # Each column in row
            row = [this.convertDigit(td.text.strip()) for td in td_elems]

            country = unidecode.unidecode(row[0])
            iso = iso_codes[country]
            total_cases = this.strip(row[1])
            new_cases = this.strip(row[2])
            total_deaths = this.strip(row[3])
            new_deaths = this.strip(row[4])
            recovered = this.strip(row[5])
            active = this.strip(row[6])
            serious = this.strip(row[7])

            sql = """INSERT INTO country_daily (day, name, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(day, country, iso, total_cases, new_cases, total_deaths, new_deaths, recovered, active, serious)
            print(sql)
            cursor.execute(sql)
            this.db.commit()
            # pd.read_sql(sql, con=connection)
            # print(str(country) + ";" + str(iso) + ";" + str(total_cases) + ";" + str(new_cases) + ";" + str(total_deaths) + ";" + str(new_deaths) + ";" + str(recovered) + ";" + str(active) + ";" + str(serious))
            # print('"%s": "%s",') % (country, iso.alpha_2 if iso else " ");
            # data.append([this.convertDigit(td.text.strip()) for td in td_elems])

        # np_array = np.array(data)
        # print(np_array)



bot = Coronavirus()
bot.get_data()
# bot.create_table()
