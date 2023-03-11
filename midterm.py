from models.config import Session #You would import this from your config file
from models.countries import Country
from models.country_totals import CountryTotals

import pandas as pd
from datetime import datetime
import pendulum
import requests




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

    country_name = country_list[0]['Slug']
    #status = requests.get(f"https://api.covid19api.com/country/{country_name}/status/confirmed?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z")
    x = requests.get(f"https://api.covid19api.com/country/{country_name}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z")
    print(f"COUNTRY TOTALS FOR: {country_name}")
    totals = x.json()
    print(totals[0])

    #print(f"STATUS SECTION REQUEST FOR: {country_name}")
    #stat = status.json()
   # print(stat[0])

    country = totals[0]
   # session = Session()
    

   # country_total = CountryTotals(country_id=country['ID'],
    #                                province=country['Province'],
     #                               city=country['City'],
      #                              city_code=country['CityCode'],
       #                             lat=country['Lat'],
        #                            long=country['Lon'],
         #                           cases=country['Active'],
          #                          status_confirmed=['Confirmed'],
           #                         status_deaths=['Deaths'],
            #                        datetime=['Date']
             #                       )

   # session.add(country_total)
    #session.flush()
    #session.commit()
    #session.close()



# PULL COUNTRY LIST
x = requests.get('https://api.covid19api.com/countries')
country_list = x.json()

print(country_list[0])

# POPULATE COUNTRIES TABLE
#populate_countries_table()

# POPULATE COUNTRY TOTALS TABLE
#pop_country_totals(country_list)






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