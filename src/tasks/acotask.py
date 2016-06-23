import sys
from os import path, environ, mkdir
import argparse

# Append path of module root
module_path = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(module_path)
from src.tasks.datapreptasks import RotoDataTask
from src.aco import Colony

import pandas as pd
import luigi

class ACOTask(luigi.Task):
    """
    A luigi Task for running ACO
    """
    positions = luigi.DictParameter(significant=False)
    alpha = luigi.Parameter()
    beta = luigi.Parameter()
    evaporation_rate = luigi.Parameter()
    iters = luigi.Parameter()

    @property
    def output_path(self):
        root = environ['BASEBALL_HOME']
        date_str = environ['DATE_STR']
        filename = '{alpha}-aco_roster.p'.format(alpha=self.alpha)
        return path.join(root, 'output', date_str, filename)
    
    def requires(self):
        return RotoDataTask()

    def output(self):
        return luigi.file.LocalTarget(path=self.output_path)

    def run(self):
        data = pd.read_pickle(self.requires().pickle_filepath)
        # Make a positions_dict that isn't ordered and of type Dict
        positions_dict = {key: value for key, value in self.positions.items()}
        colony = Colony('proj_points', data, positions_dict, self.alpha,
            self.beta, self.evaporation_rate, self.iters)

        colony.run_ants()
        print "Best metric: %d" % colony.best_roster.metric_sum
        print "Roster: "
        print colony.best_roster.df

        output_dir = path.dirname(self.output_path)
        if not path.isdir(output_dir):
            mkdir(output_dir)
        colony.best_roster.df.to_pickle(self.output_path)

