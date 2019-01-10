# Import necessary libraries (substring will require a pip install)
from bs4 import BeautifulSoup as bs
import requests
import substring
import pandas as pd
import pymongo

# Initialize PyMongo to work with MongoDBs
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)

# Define database and collection
db = client.nfl_teams
collection = db.nfl_teams

# Scrape list of team names
url = 'https://www.spotrac.com/nfl/cap/'
response = requests.get(url)
soup = bs(response.text, 'html.parser')
prereq = soup.find_all('span', class_="xs-hide")

# Initialize list of teams
teams = []

# Format team names and add to list
for x in prereq:
    name = str(x)
    name = substring.substringByChar(name, startChar='>', endChar='<')
    if name.startswith('>') and name.endswith('<'):
        name = name[1:-1]
    if ":" not in name:
        teams.append(name)

# Initialize empty Dataframe
nfl_data = pd.DataFrame(columns=("Active Players", "Pos", "Base Salary", "Signing Bonus", "Roster Bonus", "Option Bonus", "Workout Bonus", "Restruc Bonus", "Misc", "Dead Cap", "Cap Hit", "Cap %"))

# Scaper function
def scraper(team_name):

    # Format team name for url
    formatted = team_name.lower()
    formatted = formatted.replace(" ", "-")
    
    # Set up url
    url = "https://www.spotrac.com/nfl/" + formatted + "/cap"
    
    # Scrape relevant tables and combine them
    table = pd.read_html(url, match = "Active Players")
    team_table = table[0]
    
    # Rename some columns for various reasons
    team_table.rename(index=str, columns={"Pos.":"Pos", "Restruc. Bonus":"Restruc Bonus", "Misc.":"Misc"}, inplace=True)
    team_table.rename(index=str, columns={team_table.iloc[:,0].name:"Active Players"}, inplace=True)
    
    # Add team name column
    team_table["Team"] = team

    # Return the table
    return team_table

# Loop through teams list, calling scraper on each
for team in teams:
    
    # Add the team table onto the end of the nfl_data df
    nfl_data = pd.concat([nfl_data, scraper(team)])

# Read in the positions csv
position_map = pd.read_csv("positions.csv")

# Join position csv onto main csv
nfl_data = nfl_data.merge(position_map, on = "Pos")

# Insert results into Mongodb
db.collection.insert_many(nfl_data.to_dict("records"))

# This will show up if it worked, make sure to check MongoCompass for the documents as well
print("Success!")