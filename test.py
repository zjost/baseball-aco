#/usr/bin/env python
import os

import argparse
import pandas as pd

from src.datagathering import RotoBattingData, RotoPitchingData 
from src.aco import Colony
from src.fanduel_test import load_roto_data, load_dk_data

class GamedayData(object):
    def __init__(self, date_str):
        self.batting = load_dk_data(date_str, True)
        self.pitching = load_dk_data(date_str, False)

    @property
    def all_data(self):
        columns = ["name", "position", "team", "salary", "proj_points", "opposingTeam"]

        data = self.batting[self.batting.order>0][columns].append(self.pitching[columns], 
            ignore_index=True)

        return data

    def get_batters(self, AB_MIN=50, whitelist_teams=None, blacklist_teams=None):

        columns = ["name", "position", "team", "salary", "weightedOba", 
                   "proj_points", "opposingTeam"]

        data = self.batting[(self.batting.order>0) & (self.batting.ab >=AB_MIN)][columns]
        nb_players = data.copy(deep=True) # non-blacklist players

        if whitelist_teams:
            nb_players = nb_players[nb_players.team.isin(whitelist_teams)]

        if blacklist_teams:
            nb_players = nb_players[~(nb_players.team.isin(blacklist_teams))]

        return nb_players

def remove_blacklist_players(df, blacklist_teams):
    if blacklist_teams:
        return df[~(df.team.isin(blacklist_teams))]
    else:
        return df

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('date_str', type=str, help='competition date')
    parser.add_argument('--AB_MIN', type=int, default=50,
        help='Minimum ABs for a batter to be considered')
    parser.add_argument('--iters', type=int, default=10,
        help='Number of iters to run ACO')
    parser.add_argument('-w', '--whitelist_teams', action='append')
    parser.add_argument('-b', '--blacklist_teams', action='append')
    
    args = parser.parse_args()

    data = GamedayData(args.date_str)
    # Build colony and run on 'proj_points' to get pitchers
    positions = {"P1" : "SP", "P2" : "SP", "C" : "C", "1B" : "1B", "2B" : "2B", "SS": "SS", 
            "3B" : "3B", "OF1" : "OF", "OF2" : "OF", "OF3" : "OF"}

    colony = Colony("proj_points", 
                    remove_blacklist_players(data.all_data, args.blacklist_teams), 
                    positions, alpha = 1.2, beta = 0.5, 
                    evaporation_rate = 0.05, iters=args.iters)

    colony.run_ants()

    roster = colony.best_roster.df
    pitchers = roster[roster.position=="SP"]

    # Run again for batters using 'weightedOba' and salary= cap - pitcher_salary
    pitcher_salary = sum(pitchers.salary)
    positions = {"C" : "C", "1B" : "1B", "2B" : "2B", "SS": "SS", 
            "3B" : "3B", "OF1" : "OF", "OF2" : "OF", "OF3" : "OF"}

    batters = data.get_batters(args.AB_MIN, args.whitelist_teams, args.blacklist_teams)
    salary = colony.max_salary - pitcher_salary

    battercolony = Colony("weightedOba", batters, positions, alpha = 1.2, 
        beta = 0.5, evaporation_rate = 0.05, iters=args.iters)
    battercolony.max_salary = salary
    battercolony.q = 0.35
    battercolony.run_ants()

    roster_bat = battercolony.best_roster

    print roster_bat.metric_sum

    final_roster = roster_bat.df.append(pitchers, ignore_index=True)
    sorter = ['SP', 'C', '1B', '2B', '3B', 'SS', 'OF']
    final_roster['position'] = pd.Categorical(
        final_roster['position'], categories=sorter, ordered=True)
    
    print final_roster.sort_values(by='position')
    prompt = '> y/n? '
    print 'Keep this roster? '
    response = raw_input(prompt)
    if response=='y' or response=="Y":
        filepath = 'data/draftkings/' + args.date_str + '/roster.p'
        final_roster.sort_values(by='position').to_pickle(filepath)
        print 'Saved roster to %s' % filepath
        filepath = 'data/draftkings/' + args.date_str + '/roster.csv'
        with open(filepath, 'w') as f:
            f.write(','.join(final_roster.sort_values(by='position').name.values))

if __name__ == "__main__":
    main()
