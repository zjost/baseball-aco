import sys
from os import path, environ
import argparse
from collections import OrderedDict

# Append path of module root
module_path = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(module_path)
from src.tasks.acotask import ACOTask

import numpy as np
import luigi
from luigi.parameter import FrozenOrderedDict

def main():
    parser = argparse.ArgumentParser(description='Build a fanduel workflow')
    parser.add_argument('date', type=str, 
        help='the date of the roto html data')
    args = parser.parse_args()

    # Build ACO task(s)
    positions = {"P1" : "P", "C" : "C", "1B" : "1B", "2B" : "2B", "SS": "SS",
        "3B" : "3B", "OF1" : "OF", "OF2" : "OF", "OF3" : "OF"}
    #alpha = 1.2
    beta = 0.5
    evaporation_rate = 0.05
    iters = 100

    alpha_list = np.arange(0.6, 1.7, 0.1) # [0.6, 0.7, ..., 1.6]

    acotasks = [ACOTask(
        positions=FrozenOrderedDict(positions),
        alpha=alpha,
        beta=beta,
        evaporation_rate=evaporation_rate,
        iters=iters) for alpha in alpha_list]

    luigi.build(acotasks)

if __name__ == "__main__":
    main()

