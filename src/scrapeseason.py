import pandas as pd
import numpy as np
from scipy.stats import ttest_ind, mannwhitneyu, kruskal
from bs4 import BeautifulSoup
import urllib
import re

ROOT = 'http://www.baseball-reference.com'

"""
header_map = {}
for idx, th in enumerate(soup.find_all('th')):
    if idx>37:
        break
    if idx==5:
        header_map[idx] = "Away"
    else:
        header_map[idx] = str(th.text)
"""

header_map = {
    0: 'Rk', 1: 'Gcar', 2: 'Gtm', 3: 'Date', 4: 'Tm', 5: 'Away', 6: 'Opp', 7: 'Rslt',
    8: 'Inngs', 9: 'PA', 10: 'AB', 11: 'R', 12: 'H', 13: '2B', 14: '3B', 15: 'HR',
    16: 'RBI', 17: 'BB', 18: 'IBB', 19: 'SO', 20: 'HBP', 21: 'SH', 22: 'SF', 23: 'ROE',
    24: 'GDP', 25: 'SB', 26: 'CS', 27: 'BA', 28: 'OBP', 29: 'SLG', 30: 'OPS', 31: 'BOP',
    32: 'aLI', 33: 'WPA', 34: 'RE24', 35: 'DFS(DK)', 36: 'DFS(FD)', 37: 'Pos'}

def get_soup(url):
    """
    Returns BeautifulSoup object of the HTML located at url
    """
    r = urllib.urlopen(url).read()
    return BeautifulSoup(r, 'lxml')

def get_teams(year):
    """
    Returns a list of url patterns to access individual team rosters
    for a particular season:  e.g. '/teams/LAA/2016.shtml'
    """
    # Get general team links
    team_links = []
    url = ROOT + '/teams/'
    soup = get_soup(url)
    for dat in soup.find('tbody').find_all('tr', {'class': ""}):
	link = dat.find('a')['href']
	if link not in team_links:
	    team_links.append(link)

    # Get link for team players for particular season
    season_links = []
    for link in team_links:
        soup = get_soup(ROOT + link)
        season_links.append(soup.find('td', text=year).a['href'])

    return season_links

def get_batter_info(team_link):
    """
    Returns a dict of type: {"player_id" : "player_name"}
    and a list of lefty players' names
    """    
    soup = get_soup(ROOT + team_link)
    link_dict = dict() 
    lefty_list = []
    for player in soup.find("table", {"id": "team_batting"}).find_all('a'):
	position = player.findPrevious('td').findPrevious('td').text
	if position=="P":
	    continue
	else:
	    link_dict[player.text] = player['href']
	    if re.search('\*', player.findParent().text):
		lefty_list.append(player.text)
    
    player_ids = dict() # Type: {"player_id: "player_name"}
    for player, link in link_dict.items():
        try:
            player_id = re.match("(\/players\/\w\/)(\w*)(\.shtml)", link).group(2)
            player_ids[player_id] = player
        except:
            print "Didn't find player_id for %s at %s" % (player, link)
    
    return player_ids, lefty_list

def get_player_page(player_id, year):
    """ Returns hyperlink to individual player stats page for given year """
    root = "http://www.baseball-reference.com/players/"
    query = "gl.cgi?id={player_id}&t=b&year={year}".format(
        player_id=player_id, year=year)
    
    return root+query

def extract_row_data(rows):
    # Initialize "data" to be of type {key: []}
    data = {}
    for k, v in header_map.items():
        data[v] = list()

    for row in rows:
        row_data = row.find_all('td')
        if row_data:
            # Make sure not an empty row
            if row_data[0].text=="":
                continue
            # Iterate over the data
            else:
                for idx, td in enumerate(row_data):
                    data[header_map[idx]].append(td.text)
                    
    return data

def get_singles(data):
    singles = data["H"] - data["2B"] - data["3B"] - data["HR"]
    return singles

def get_teams_season_data(player_ids, lefty_list):
    master_df = pd.DataFrame()

    for player_id, player_name in player_ids.items():
	player_page = get_player_page(player_id, 2015)
	soup = get_soup(player_page) 
	
	try:
	    rows = soup.find('table', {'id': 'batting_gamelogs'}).find(
		'tbody').find_all('tr')
	except AttributeError:
	    print 'No data for %s' % player_name
            continue
	    
	data_dict = extract_row_data(rows)
	
	df = pd.DataFrame(data_dict)
	df['player_id'] = player_id
	df['Name'] = player_name

	# Filter out games he didn't start
	df = df[(df['Inngs'].str.contains('GS')) | (
	    df['Inngs'].str.contains('CG'))]
	
	# Change Away column to boolean
	df.Away[df.Away=="@"] = 1.0
	df.Away[pd.isnull(df.Away)] = 0.0
	
	if player_name in lefty_list:
	    hand = "L"
	else:
	    hand = "R"
	
	df['hand'] = hand
	
	master_df = master_df.append(df, ignore_index=True)

    float_columns = ['2B', '3B', 'AB', 'BA', 'BB', 'BOP', 'CS', 'DFS(DK)',
	   'DFS(FD)', 'GDP', 'Gcar', 'H', 'HBP', 'HR', 'IBB',
	   'OBP', 'OPS', 'PA', 'R', 'RBI', 'RE24',
	   'ROE', 'Rk', 'SB', 'SF', 'SH', 'SLG', 'SO', 'WPA', 'aLI']

    for column in float_columns:
	try:
	    master_df[column] = master_df[column].replace(
		r'', np.nan, regex=True)
	    master_df[column] = master_df[column].astype(float)
	except ValueError:
	    print "Can't convert %s to float" % column

    return master_df

def add_dk_scores(df):
    # Add singles column
    df['1B'] = get_singles(df)
    scoring_dict = {'1B': 3, "2B": 5, "3B": 8, "RBI": 2, "BB": 2, 
	"HBP": 2, "HR": 10, "R": 2, "SB": 5}
    df['DKscore'] = sum(
	[df[column]*scoring_dict[column] for column in scoring_dict.keys()])

    return df

def join_pitcher_data(df, team_link, year):

    team = team_link.split('/')[2]
    url = ROOT + '/teams/tgl.cgi?team={team}&t=b&year={year}'.format(
	team=team, year=year)
    soup = get_soup(url)

    # Extract headers
    headers = {}
    th = soup.find_all('th')
    for idx, header in enumerate(th[:32]):
	if idx==3:
	    headers[idx] = 'Away'
	else:
	    headers[idx] = str(header.text)

    data = {} # Will be type: {"col": data}
    for idx, row in enumerate(soup.find('tbody').find_all('tr')):
	data_list = []
	if row.find('td'):
	    for dat in row.find_all('td'):
		data_list.append(dat.text)
	    data[idx] = data_list
	    
    gamelogs = pd.DataFrame(data).T.rename(columns=headers)

    # Change "Away" "@" symbols to boolean
    gamelogs.loc[gamelogs.Away=='','Away'] = 0.0
    gamelogs.loc[gamelogs.Away=='@', 'Away'] = 1.0

    return df.merge(
	gamelogs[['Date', 'Thr', 'Opp. Starter (GmeSc)']], on='Date')

def main():
    year = "2015"
    team_data_links = get_teams('2016') # list: ['/teams/LAA/2016.shtml']
    final_df = pd.DataFrame()
    for team_link in team_data_links:
        player_ids, lefty_list = get_batter_info(team_link)
        df = get_teams_season_data(player_ids, lefty_list)
        df = add_dk_scores(df)
        df = join_pitcher_data(df, team_link, year)
        final_df = final_df.append(df, ignore_index=True)

    final_df.to_pickle('final_df.p')
    print final_df.head(50)

if __name__ == '__main__':
    main()
