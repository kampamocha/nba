# NBA Data

## Contents

- README.md   
  This file.
- .env.example  
  Environment variables for MySQL access. This file should be renamed as .env and fill accordingly.
- Data_Exploration.ipynb  
  Jupyter notebook to initial data exploration
- data_helpers  
  Python file with helper functions for SQL and data manipulation
- read_and_dump.py  
  File to read data from csv files, transform and dump into MySQL server
- best_players.py 
  Program to retrieve most productive players by week at any given season
- predict_game.py 
  Program to predict result between two teams at any given seasons
- data/ 
  Folder containing compressed file of input data
  
  
## Running the programs

For any of the following three files you can see help with -h option

### Dump data into MySQL
#### Usage  
python read_and_dump.py -d <data_dir>

#### Example  
python read_and_dump.py -d data

### Retrieving best players
#### Usage  
python best_players.py -s <season>

#### Example  
python best_players.py -s 2011

### Predict games
Note that you can predict how would be a game between teams from different seasons or even the same team as local or visitor.
The result is adjusted for home team advantage and estimated points according to seasons baseline.

#### Usage 
predict_game.py -s <home_season> -m <home_team> -t <away_season> -a <away_team>

#### Example  
predict_game.py -s 2017 -m GSW -t 2109 -a LAL

