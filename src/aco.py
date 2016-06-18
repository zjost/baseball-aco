"""
Module containing classes for running Ant Colony Optimization
on a DataFrame of players in a variety of positions.  It 
attempts to find the best roster of players to maximize the total
value of a metric that's specified as a column in the DataFrame
while adhering to a salary cap
"""

import pandas as pd

class Ant(object):
    """ 
    Object that builds a roster using ACO
    
    Args:
        positions (Dict): A dictionary that maps position name to the column
            value that indicates that position in the data set.
            i.e. {"Outfielder1" : "OF"} if the df used "OF"
        metric (str): The column name of the data frame containing the metric
        max_salary (float): Salary cap
        data (pd.DataFrame): DataFrame containing candidate data
        q (float): Initial pheromone amount.
        alpha (float): Exponent that scales the pheromones' effect on
            calculating the probabilities
        belta (float): Exponent that scales the greedy factory when
            calculating the probabilities


    """
        # XXX:TODO Modify the probability calculation to have a third term
        # that is completely random.  
        
        
    
    def __init__(self, positions, metric, max_salary, data, q, alpha, beta):
        self.positions = copy(positions)
        self.remaining_positions = self.positions
        self.roster = pd.DataFrame()
        self.metric = metric
        self.max_salary = max_salary
        self.data = deepcopy(data)
        self.q = q
        self.alpha = alpha
        self.beta = beta
    
    def get_candidates(self, position_column, position_key, salary_column, salary):
        
        # XXX:TODO Remove from Ant class?
        """
        Returns a copy of the dataframe that only contains players in the
        specified position and whose salary is below that passed in

        Args:
            position_column (str): Column name in dataframe that contains 
                the position field
            position_key (str): The position you want that are the keys in 
                self.positions
            salary_column (str): Column name in dataframe that contains the
                salary field
            salary (float): The remaining salary
        """
        candidate_position = self.positions[position_key]
        return deepcopy(self.data[
            (self.data[position_column]==candidate_position) & \
            (self.data[salary_column] <= salary)])
    
    def calculate_probabilities(self, candidate_df):
        """
        Calculates probabilities by weighting the pheromone effect and
        greedy values by exponents self.alpha and self.beta.
        
        Returns a column of probabilities
        """

        # Normalizing factor
        norm = sum(
            candidate_df["pheromones"]**self.alpha + \
            candidate_df[self.metric]**self.beta)
        
        probabilities = (candidate_df.loc[:, "pheromones"]**self.alpha + \
            candidate_df.loc[:,self.metric]**self.beta)/norm

        return deepcopy(probabilities)
    
def choose_candidate(candidate_df):
    """
    Chooses from the available candidates using weights in the
    probability column

    If there are no candidates, just returns the empty DataFrame
    """
    if len(candidate_df) > 0:
        try:
            return candidate_df.sample(1, weights="probabilities")
        except:
            print "Problem choosing a candidate. Check the probabilities!"
            print "Candidate df = "
            print candidate_df
    else:
        return candidate_df

def build_roster(ant):
    """
    Builds a Roster from an ant
    """
    roster = pd.DataFrame() 
    ant.remaining_salary = ant.max_salary
    for i in range(len(ant.positions)):
        # Get a player position to fill
        position = random.choice(ant.positions.keys())
        # Get a candidate list (a deep copy of the filtered self.data)
        candidates = ant.get_candidates(
            "position", position, "salary", ant.remaining_salary)
        
        # If there are players in the roster, drop them from candidate
        # DataFrame to avoid redundant choices
        if not roster.empty:
            candidates.drop(roster.index, inplace=True, errors='ignore')
        
        # If no eligable candidates, return the empty DataFrame, 
        # indicating a Null roster
        if len(candidates) == 0:
            return candidates
            
        candidates['probabilities'] = ant.calculate_probabilities(candidates)
        # Choose a candidate
        candidate = ant.choose_candidate(candidates)
        roster = roster.append(candidate)
        # Remove position from future options
        ant.positions.pop(position)
        # Update salary
        ant.remaining_salary -= candidate.salary.values[0]
        
    return Roster(roster, ant.metric)

class Roster(object):
    """
    An object that represents a player roster
    
    Inputs
        players (pd.DataFrame): A DataFrame of players
        metric (str): String indicating the column in players that contains
            the optimization metric
    """
    def __init__(self, players, metric):
        self.df = players
        self.metric = metric

    def __len__(self):
        return len(self.df)

    def __getitem__(self, val):
        return self.df[val]

    @property
    def metric_sum(self):
        return sum(self.df[self.metric].values)

class Colony(object):
    
    number_of = 3.0 # number of outfielders
    max_salary = 35000
    
    q = 10.0
    boost_amount = 2.0
    
    def __init__(self, metric, data, positions, alpha, beta, evaporation_rate, iters):
        self.metric = metric
        self.data = deepcopy(data)
        self.positions = copy(positions)
        self.alpha = alpha
        self.beta = beta
        self.evaporation_rate = evaporation_rate
        self.iters = iters
        
        self.ant_count = self.position_counts["count"].max()
        
        # Add relevant columns to dataframe
        self.initialize_pheromones()
        self.add_position_count_column()
        
    def initialize_pheromones(self):
        self.data["pheromones"] = self.q
         
    @property
    def position_counts(self):
        """ Returns dataFrame of type:
            {"position": [positions], "count": [counts]} """
        positions = []
        counts = []
        for key, value in self.positions.items():
            if value in positions:
                continue
            else:
                positions.append(value)
                counts.append(sum(self.data.position==value))
            
        return pd.DataFrame.from_dict(
            {"position": positions, "count" : counts})
        
    def add_position_count_column(self):
        """ Adds a count to self.data indicating the number 
        of players at each position
        """
        self.data = self.data.merge(self.position_counts, how="left", on="position")
        
    
    def build_ants(self):
        self.ants = list()
        [self.ants.append(Ant(self.positions, self.metric, self.max_salary, 
                              self.data, self.q, self.alpha, self.beta)) \
        for i in range(self.ant_count)]
             
    def update_tau(self, roster_list):
        """
        Need a DataFrame column/series with player_idx : pheromone update
        """
        for roster in roster_list:
            # Ignore rosters that aren't full
            if len(roster) < len(self.positions):
                continue
                
            # Get the ant_count / position_count
            n_k = self.ant_count/roster["count"]
            # Divide pheromone amount on outfielders by the number of outfielders
            n_k[roster["position"]=="OF"] *= float(self.number_of)
            # Scale the roster_metric by n_k, self.q, number of positions
            #delta_tau_k = (ant.roster_metric/(self.q*len(self.positions)))**2*(1/n_k)
            delta_tau_k = (roster.metric_sum/(self.q*len(self.positions)))*(1/n_k)
            # Update the global pheromones for only these roster elements
            self.data.ix[delta_tau_k.index, 'pheromones'] += delta_tau_k
            
    def evaporate(self):
        self.data.loc[:, "pheromones"] = self.data["pheromones"]*\
            (1-self.evaporation_rate)
        
    def turbo_boost(self, roster_df):
        """
        This multiplies the pheromones count for a roster by a constant
        """
        self.data.ix[roster_df.index, "pheromones"] *= self.boost_amount
        # Reset anything greater than the max to equal the max to prevent
        #exponential growth
        #self.data.ix[(roster["pheromones"]>=self.pheromone_max).index, 
        #   "pheromones"] = self.pheromone_max
            
    def run_ants(self):
        j = 0
        self.history = {}
        self.best_roster = pd.DataFrame()
        self.best_roster_metric = 0
        self.best_history = {}
        
        while j < self.iters:
            self.build_ants()
            self.history[j] = {}
            pool = Pool()
            results = pool.map(build_roster, self.ants) #list of Roster objects
            pool.close()
            pool.join()

            for roster in results:
                if roster.metric_sum > self.best_roster_metric:
                    self.best_roster_metric = roster_metric
                    self.best_roster = roster
                    self.turbo_boost(roster.df)
                
                self.history[j][i] = roster.metric_sum
    
            self.best_history[j] = self.best_roster_metric
            self.update_tau(results)
            self.evaporate()    
            j += 1
            
