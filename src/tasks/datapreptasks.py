import sys
from os import path, environ, mkdir
# Append path of module root
module_path = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(module_path)
from src.datagathering import RotoBattingData, RotoPitchingData

import pandas as pd
import luigi

class HtmlDataTask(luigi.ExternalTask):
    """
    An external task to check for file existance for html player data
    """
    html_filepath = luigi.Parameter()
    def output(self):
        return luigi.file.LocalTarget(path=self.html_filepath)

class RotoDataTask(luigi.Task):
    """
    A task for generating a pickled DataFrame of players from an html file
    """

    @property
    def html_file_dir(self):
        root = environ['BASEBALL_HOME']
        date_str = environ['DATE_STR']
        return path.join(root, 'data', 'roto', date_str)

    @property
    def pickle_filepath(self):
        root = environ['BASEBALL_HOME']
        date_str = environ['DATE_STR']
        return path.join(root, 'output', date_str, "player_df.p")

    @property
    def html_filepaths(self):
        """
        Returns a list of filepaths to the html data files
        """
        filenames = ['batting.html', 'pitching.html']
        return [path.join(self.html_file_dir, name) for name in filenames]

    def output(self):
        return luigi.file.LocalTarget(path=self.pickle_filepath)

    def requires(self):
        return [HtmlDataTask(html_filepath=fp) for fp in self.html_filepaths]

    def run(self):
        """
        Build the DataFrame from the html files and pickle it
        """
        positions = {"P1" : "P", "C" : "C", "1B" : "1B", "2B" : "2B", "SS": "SS", 
            "3B" : "3B", "OF1" : "OF", "OF2" : "OF", "OF3" : "OF"}

        batting_df = RotoBattingData(self.html_filepaths[0]).df
        pitching_df = RotoPitchingData(self.html_filepaths[1]).df
        
        # Columns to filter to
        columns = ["name", "position", "team", "salary", "proj_points"]

        data = batting_df[batting_df.order>0][columns].append(
            pitching_df[columns], ignore_index=True)

        output_dir = path.dirname(self.pickle_filepath)
        if not path.isdir(output_dir):
            mkdir(output_dir)
        data.to_pickle(self.pickle_filepath) 
