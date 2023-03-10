from models.config import Session #You would import this from your config file
from models.countries import Country

import pandas as pd
from datetime import datetime
import pendulum
import requests




x = requests.get('https://api.covid19api.com/countries')

country_x = x.json()
print("FIRST COUNTRY")
print(country_x[0])




session = Session()
# This will retrieve all of the users from the database 
# (It'll be a list, so you may have 100 users or 0 users)
test = session.query(Country).all() 
print(f"TEST: {test}")


# This will retrieve the user who's username is NASA
#nasaUser = session.query(User).filter(User.username == "NASA").first()

#You can then print the username of the user you retrieved
#print(nasaUser.username)

#We recommend that you reassign the user to a variable so that you can use it later
#nasaUsername = nasaUser.username

# This will close the session that you opened at the beginning of the file.
session.close()