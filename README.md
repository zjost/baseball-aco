Although this repo is in desperate need of a proper README and documentation, the priority for delivery will depend on demand (for which now there is none).  Feel free to post questions/requests as an Issue and I'll do my best to accommodate.

The current, albiet sub-optimal, process is to navigate to the relevant [rotogrinders.com](http://www.rotogrinders.com) page to load current data, do relevant filtering (i.e. time slot), then save the html file locally.  It needs to be saved in sub-directory structure: `<this_repo>/data/draftkings/<date_string>/<filename>` with filename given by either `pitching.html` or `batting.html`.

The date string is then specified when running the program, e.g.:  `python test.py 10-13-16`.  You can see optional argument details via `python test.py --help`.  

The real meat of the algorithm is in `src/aco.py`.  The `test.py` script is just an example of a driver program leveraging these classes.  
