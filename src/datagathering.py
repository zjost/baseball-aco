""" 
A module containing the classes for extracting useful data from 
different web sources of baseball data.
"""

from copy import copy, deepcopy
import random

import urllib
import pandas as pd
from bs4 import BeautifulSoup

def load_html_page(filepath):
    """ 
    Loads a saved html document and returns a BeautifulSoup representation
    """
    
    r = ""
    with open(filepath, 'r') as f:
        r = f.read()
    
    soup = BeautifulSoup(r, "lxml")
    
    return soup

class RotoBattingData(object):
    """ A class for handling data from RotoGrinders"""
    div_map = {
        "name" : "Name",
        "salary" : "Salary",
        "team" : "Team",
        "homeBool" : "Home?",
        "position" : "Position",
        "handedness" : "Hand",
        "order" : "Order",
        "opposingTeam" : "Opp",
        "opposingPitcher" : "Pitcher",
        "opposingPitcherHand" : "PHND",
        "platoonAdvantage" : "PLTN?",
        "vegasTotal" : "Total",
        "vegasMovement" : "Movement",
        "ab" : "AB",
        "batAvg" : "AVG",
        "weightedOba" : "wOBA",
        "iso" : "ISO",
        "obp" : "OBP",
        "slg" : "SLG",
        "ops" : "OPS",
        "proj_points" : "Points",
        "points_p_$1k" : "Pt/$/K"
    }
    
    # Columns that should have Boolean type
    bool_columns = ["homeBool", "platoonAdvantage"]
    # Columns that should have float type
    float_columns = ["vegasTotal", "vegasMovement", "ab", "batAvg", 
                     "weightedOba","iso", "obp", "slg", "ops", "proj_points", 
                     "points_p_$1k", "order"]
    # Columns that represent percentages
    pct_columns = []
    
    
    def __init__(self, data_filepath):
        self.soup = load_html_page(data_filepath)
        # Set base attributes
        for key, div_string in self.div_map.items():
            setattr(self, key, self.get_column(div_string))
        
        # Convert strings to appropriate data types
        self.update_salary()
        for column in self.bool_columns:
            self.update_bool(column)
        
        for column in self.float_columns:
            self.update_float(column)
            
        for column in self.pct_columns:
            self.update_pct(column)
        
    @property
    def df(self):
        """ A pandas dataframe representation of the data """
        idx = range(len(self.name))
        data = dict()
        for i in idx:
            data[i] = {key: getattr(self, key)[i] for key in self.div_map}
            
        return pd.DataFrame.from_dict(data, orient="index")
        
    
    def update_salary(self):
        """ Converts string type of "$1,000" to float 1000.0 """
        new_salaries = list()
        for salary in self.salary:
            new_salaries.append(
                float(salary.strip().replace("K", "").replace("$", ""))*1000)
        
        self.salary = new_salaries
        
    def update_bool(self, attribute):
        """ Converts string type to int """
        new = list()
        # Check for their unicode for Null
        for item in getattr(self, attribute):
            if item== u'\xa0':
                new.append(0)
            else:
                new.append(1)
            
        setattr(self, attribute, new)
        
    def update_float(self, attribute):
        """ Converts string type to int """
        new = list()
        for item in getattr(self, attribute):
            # Check for common non-numerics
            if item == u'\xa0' or item=="N/A":
                new.append(0)
            else:
                try:
                    new.append(float(item.strip()))
                except:
                    print "Failed to convert float for %s" % item
            
        setattr(self, attribute, new)
        
    def update_pct(self, attribute):
        """ Converts string type to percent """
        new = list()
        for item in getattr(self, attribute):
            # Check if the unicode representing a null is present
            if item == u'\xa0':
                new.append(0)
            else:
                new.append(float(item.strip().replace("%", "")))
            
        setattr(self, attribute, new)
    
    def get_column(self, find_string):
        """ 
        Returns the column as a list of values 

        Process:
            Searches the html document for the column header name
            and then iterates over its siblings to get the data.  
            Returns the values as a list.
        
        Args:
            find_string (str): The text string in the column header div

        Returns:
            item_list (List[str]): A list of data elements in column labeled by 
            "find_string"
        """

        try:
            # Gets the element immediately after the column name
            first_element = self.soup.find(text=find_string).findNext('div')
        except:
            print "Error!  Couldn't find column: %s " % find_string
        
        # Uses "findNextSiblings" method to return list of html elements
        element_list = [first_element] + first_element.findNextSiblings()
        
        # Convert from BeautifulSoup html elements to text strings
        item_list = list()
        
        for element in element_list:
            item_list.append(element.text)
        
        return item_list


class RotoPitchingData(RotoBattingData):
    """ 
    A tweak on RotoBattingData that contains columns and types
    used to represent the data for pitchers
    """
    div_map = {
        "name" : "Name",
        "salary" : "Salary",
        "team" : "Team",
        "homeBool" : "Home?",
        "position" : "Position",
        "handedness" : "Hand",
        "opposingTeam" : "Opp",
        "vegasOU" : "O/U",
        "vegasLine" : "Line",
        "vegasTotal" : "Total",
        "vegasMovement" : "Movement",
        "numL" : "xL",
        "leftWOba" : "LwOBA",
        "leftIso" : "LISO",
        "leftKp9" : "LK/9",
        "numR" : "xR",
        "rightWOba" : "RwOBA",
        "rightIso" : "RISO",
        "rightKp9" : "RK/9",
        "games" : "GP",
        "skill_Era" : "SIERA",
        "def_ind_Era" : "xFIP",
        "hr_v_flyball" : "HR/FB",
        "exp_WOba" : "xWOBA",
        "exp_K9" : "xK/9",
        "gb_pct" : "GB%",
        "flyball_pct" : "FB%",
        "proj_points" : "Points",
        "points_p_$1k" : "Pt/$/K"
    }
    
    bool_columns = ["homeBool"]
    
    float_columns = [
        "vegasOU", "vegasLine", "vegasTotal", "vegasMovement", 
        "numL", "leftWOba", "leftIso", "leftKp9", "numR", 
        "rightWOba", "rightIso", "rightKp9", "games", 
        "skill_Era", "def_ind_Era", "exp_WOba", "exp_K9", 
        "proj_points", "points_p_$1k"]
    
    pct_columns = ["hr_v_flyball", "gb_pct", "flyball_pct"]


class YahooData(object):
    """ 
    A class for handling data from Yahoo Daily Fantasy Sports
    
    Args:
        data_filepath (str): Filepath containing html file

    """

    # A dictionary using structure:
    # {"column_name": {"attribute_name": "attribute_value"}}
    # that represents the identifier type and character string
    # for the desired data type
    html_labels = {
        "position": {"data-tst": "player-pos"},
        "name": {"class": "Td-n"},
        "salary": {"data-tst": "player-salary"},
        "fppg": {"data-tst": "player-fppg"},
        "matchup": {"data-tst": "player-matchup"}}
    
    def __init__(self, data_filepath):
        
        self.soup = load_html_page(data_filepath)
        self.df = self.build_df()

        self.update_salary()
        self.update_float("fppg")
    
    def build_df(self):
        """
        Loops over the divs containing player stats by searching using
        the attributes in the "self.html_labels" attribute map.  Then
        filters out non-starting players.

        Returns
            filtered_df (pandas.DataFrame): DataFrame of starting players
                with columns for the various stats
        """
        player_dict = {} # Structure: {player_idx : {stat_name : stat_value}}

        # Get list of html elements, each of which contain the stats
        # for a single player
        players = self.soup.find_all("tr", class_="Cur-p")
        
        for idx, player in enumerate(players):
            player_stats = {} # Structure: {stat_name : stat_value}
            
            # Get the stats using the attribute map in self.html_labels
            # Ex:  <div "attribute"=value>Thing I want</div> 
            # is mapped via: {"attribute" : value}
            for key, attrs in self.html_labels.items():
                player_stats[key] = player.find(attrs=attrs).text 
            
            # Handle "team" separately since it doesn't match the pattern
            # used in the above loop
            team = {
                "team": player.find(attrs=self.html_labels["matchup"]).find(
                    attrs={"class": "Fw-b"}).text}
            
            player_stats.update(team)

            player_dict[idx] = player_stats
            
        df = pd.DataFrame.from_dict(player_dict, orient='index')

        # Filter df to remove players that are not believed to be in 
        # the starting lineup
        filtered_df = self.filter_to_starters(df)

        return filtered_df
    
    def update_salary(self):
        """ Converts string type of $1,000 to float"""
        new_salary = self.df.loc[:, "salary"].str.replace("$", "")
        self.df.loc[:, "salary"] = new_salary
        self.update_float("salary")
        
    def update_float(self, column_name):
        new_column = self.df.loc[:, column_name].astype(float)
        self.df.loc[:, column_name] = new_column
        
    def filter_to_starters(self, df):
        """ Checks the html for an indication of starting """
        starting_list = []
        for player_row in self.soup.find_all("tr", {"class" : "Cur-p"}):
            name = player_row.find("a", {"class" : "Td-n"}).text
            # Check if indicating a batter is starting
            if player_row.find("i", {"data-tst" : "player-is-in-lineup"}):
                starting_list.append(name)
            # Check if inicatinga pitcher is starting
            if player_row.find("i", {"data-tst" : "player-is-starting"}):
                starting_list.append(name)
                
        return df[df.name.isin(starting_list)]
