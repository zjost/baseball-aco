
import os

from datagathering import RotoBattingData, RotoPitchingData 
from aco import Colony


def load_roto_data(date_str, batting_bool):
    """
    Loads the relevant Roto Data class and returns the pandas dataframe
    """
    parent_dir = os.path.join(os.path.dirname(__file__), os.pardir)
    file_dir = os.path.join(parent_dir, 'data', 'roto', date_str)
    if batting_bool:
        roto = RotoBattingData(os.path.join(file_dir, "batting.html"))
    else:
        roto = RotoPitchingData(os.path.join(file_dir, "pitching.html"))

    return roto.df

def load_dk_data(date_str, batting_bool):
    """
    Loads the relevant Roto Data class and returns the pandas dataframe
    """
    parent_dir = os.path.join(os.path.dirname(__file__), os.pardir)
    file_dir = os.path.join(parent_dir, 'data', 'draftkings', date_str)
    if batting_bool:
        roto = RotoBattingData(os.path.join(file_dir, "batting.html"))
    else:
        roto = RotoPitchingData(os.path.join(file_dir, "pitching.html"))

    return roto.df

if __name__ == "__main__":

    positions = {"P1" : "P", "C" : "C", "1B" : "1B", "2B" : "2B", "SS": "SS", 
        "3B" : "3B", "OF1" : "OF", "OF2" : "OF", "OF3" : "OF"}

    date_str = "06-17-16"
    batting = load_roto_data(date_str, True)
    pitching = load_roto_data(date_str, False)
    # Columns to filter to
    columns = ["name", "position", "team", "salary", "proj_points"]

    data = batting[batting.order>0][columns].append(pitching[columns], 
        ignore_index=True)

    #print data.head()

    colony = Colony("proj_points", data, positions, alpha = 1.2, 
        beta = 0.5, evaporation_rate = 0.05, iters=10)
    
    colony.run_ants()
    print colony.best_roster.df
