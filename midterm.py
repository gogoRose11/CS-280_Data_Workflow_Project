from models.config import Session #You would import this from your config file
from models.countries import Country

import pandas as pd
from datetime import datetime
import pendulum
import requests




def populate_countries_table():

    x = requests.get('https://api.covid19api.com/countries')

    country_list = x.json()
    session = Session()
    session.flush()
    print(f"NUM COUNTRIES: {len(country_list)}")

    for i in range(len(country_list)):
        country = Country(country=country_list[i]['Country'],
                            slug=country_list[i]['Slug'],
                            iso2=country_list[i]['ISO2'])
        session.add(country)

    session.commit()
    session.clost()

populate_countries_table()





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