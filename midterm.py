from models.config import Session #You would import this from your config file
from models.countries import Country
from models.country_totals import CountryTotals

import pandas as pd
from datetime import datetime
import pendulum
import requests
import time




def populate_countries_table(country_list):
    session = Session()
    session.flush()
    print(f"NUM COUNTRIES: {len(country_list)}")

    for i in range(len(country_list)):
        country = Country(country=country_list[i]['Country'],
                            slug=country_list[i]['Slug'],
                            iso2=country_list[i]['ISO2'])
        session.add(country)

    session.commit()
    session.close()



def pop_country_totals(country_list):

    #time.sleep(5)

    #session = Session()
    for i in range(126, len(country_list)):
        session = Session()
        print(f"FOR LOOP ITERATION: {i}")
        # time.sleep(5)
        country_name = country_list[i]['Slug']
        #time.sleep(5)
        print(f"COUNTRY TOTALS FOR: {country_name}")
        x = requests.get(f"https://api.covid19api.com/country/{country_name}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z")
        time.sleep(5)
        totals = x.json()
        #print(totals[0])

        
        real_country = session.query(Country).filter(Country.slug == country_name).first()
        real_country_id = real_country.id

        # DO IT AS LONG AS IT ISN'T THE U.S. OR CHINA
        if country_name != 'united-states' and country_name != 'china':
            for j in range(len(totals)):

                date_string = totals[j]['Date']
                datetime_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')

                country = totals[j]
                
                country_total1 = CountryTotals(country_id=real_country_id, province=country['Province'], city=country['City'], city_code=country['CityCode'], lat=country['Lat'], long=country['Lon'], cases=country['Confirmed'], status='confirmed', datetime=datetime_obj)
                session.add(country_total1)
                country_total2 = CountryTotals(country_id=real_country_id, province=country['Province'], city=country['City'], city_code=country['CityCode'], lat=country['Lat'], long=country['Lon'], cases=country['Deaths'], status='deaths', datetime=datetime_obj)
                session.add(country_total2)
        session.commit()
        session.close()
        

        #session.flush()
    
    #session.commit()
    #session.close()



# PULL COUNTRY LIST
x = requests.get('https://api.covid19api.com/countries')
time.sleep(5)
country_list = x.json()

#print(country_list[0])

# POPULATE COUNTRIES TABLE
#populate_countries_table()

# POPULATE COUNTRY TOTALS TABLE
pop_country_totals(country_list)






# This will retrieve all of the users from the database 
# (It'll be a list, so you may have 100 users or 0 users)
#test = session.query(Country).all() 
#print(f"TEST: {test}")


# This will retrieve the user who's username is NASA
#nasaUser = session.query(User).filter(User.username == "NASA").first()

#You can then print the username of the user you retrieved
#print(nasaUser.username)

#We recommend that you reassign the user to a variable so that you can use it later
#nasaUsername = nasaUser.username

# This will close the session that you opened at the beginning of the file.
#session.close()