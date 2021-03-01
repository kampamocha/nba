"""
Created on Thu Feb 25 22:35:08 2021
@author: Diego Campos Sobrino
"""
import getopt
import sys
import data_helpers as dh

"""
DISCLAIMER:
We could use features such as:
    - Individual and team statistics such as rebounds, free_throws, blocks, etc.
    - Face-to-face matchups between teams
    - Adjusted weights for recent games
    - Injuries, etc.
for a regression or machine learning model.

To keep it simple we just use expected win percentages from offensive and
defensive point averages for home and away teams, assuming balanced schedules
(which is not the case in reality).
Then those estimates are used by the log5 formula to give a win probability
for the home team.

REFERENCES:
https://en.wikipedia.org/wiki/Pythagorean_expectation#Use_in_basketball
https://en.wikipedia.org/wiki/Log5

NOTES:

"""

#%%
"""
Estimate winning percentage given offensive points and defensive points of a team.
It is proven to be a better estimate for future performance than the actual
winning percentage.
"""
def expected_win_percent(off_points, def_points, p=14):
    return off_points ** p / (off_points ** p + def_points ** p)

"""
Estimate the probability that one team will win a game, based on winning
percentages of both teams.
"""
def log5(p1, p2):
    return (p1 - p1 * p2) / (p1 + p2 - 2 * p1 * p2)

"""
Game points estimation based on league baseline, team's offensive strenght
factor and opponent's defensive strengtht factor
"""
def estimated_points(base, off_factor, def_factor):
    return base * off_factor * def_factor


#%%
def main(argv):
    usage = """
    Usage: predict_game.py -s <home_season> -m <home_team> -t <away_season> -a <away_team>

    Example:
        To predict result of game between 2017 Golden State and the 2019 Lakers use

        predict_game.py -s 2017 -m GSW -t 2109 -a LAL

    """
    db_name = 'nba'
    home_season = 2017
    home_team = 'GSW'
    away_season = 2019
    away_team = 'LAL'

    try:
        opts, args = getopt.getopt(argv[1:], 'hs:m:t:a:', ['help', 'home_season=', 'home_team=', 'away_season', 'away_team='])
    except getopt.GetoptError as err:
        print(err)
        print(usage)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(usage)
            sys.exit()
        elif opt in ('-s', '--hseason'):
            home_season = int(arg)
        elif opt in ('-m', '--hteam'):
            home_team = arg
        elif opt in ('-t', '--aseason'):
            away_season = int(arg)
        elif opt in ('-a', '--ateam'):
            away_team = arg


#%%
    # Get MySQL credentials
    host, user, password = dh.load_mysql_credentials()

    # Create connection
    connection = dh.create_connection(host, user, password, db_name)
    if not connection:
        connection.close()
        sys.exit()

    cursor = connection.cursor()

#%%
    # Get local team data
    query = """
    SELECT AVG(PTS_home) AS AVG_Offense, AVG(PTS_away) AS AVG_Defense
    FROM games AS g
    JOIN teams AS t ON g.TEAM_ID_home = t.TEAM_ID
    WHERE season = %s AND t.ABBREVIATION = %s
    """

    cursor.execute(query, (home_season, home_team))
    home_off_avg, home_def_avg = cursor.fetchone()

    # Get away team data
    query = """
    SELECT AVG(PTS_away) AS AVG_Offense, AVG(PTS_home) AS AVG_Defense
    FROM games AS g
    JOIN teams AS t ON g.TEAM_ID_away = t.TEAM_ID
    WHERE season = %s AND t.ABBREVIATION = %s
    """

    cursor.execute(query, (away_season, away_team))
    away_off_avg, away_def_avg = cursor.fetchone()

    # Get home team season baseline
    query = """
    SELECT AVG(PTS_home) AS AVG_Offense, AVG(PTS_away) AS AVG_Defense
    FROM games AS g
    WHERE season = %s
    """

    cursor.execute(query, (home_season,))
    home_season_off_avg, home_season_def_avg = cursor.fetchone()

    # Get away team season baseline
    if home_season != away_season:

        query = """
        SELECT AVG(PTS_away) AS AVG_Offense, AVG(PTS_home) AS AVG_Defense
        FROM games AS g
        WHERE season = %s
        """

        cursor.execute(query, (away_season,))
        away_season_off_avg, away_season_def_avg = cursor.fetchone()

    else:
        away_season_off_avg, away_season_def_avg = home_season_def_avg, home_season_off_avg

#%%

    """
    Estimate win probability of home team
    """

    # Expected win pct for home and away teams
    home_exp_wp = expected_win_percent(home_off_avg, home_def_avg)
    away_exp_wp = expected_win_percent(away_off_avg, away_def_avg)

    home_win_prob = log5(home_exp_wp, away_exp_wp)

    print(f'{home_team} has {round(home_win_prob*100,1)}% of win')


    """
    Estimate expected points
    """

    # Common points  baseline for both seasons
    home_pts_base = (home_season_off_avg + away_season_def_avg) / 2
    away_pts_base = (home_season_def_avg + away_season_off_avg) / 2

    # Deviation from average for home team
    home_off_dev = home_off_avg / home_season_off_avg
    home_def_dev = home_def_avg / home_season_def_avg

    # Deviation from average for away team
    away_off_dev = away_off_avg / away_season_off_avg
    away_def_dev = away_def_avg / away_season_def_avg

    # Estimate points
    home_pts = estimated_points(home_pts_base, home_off_dev, away_def_dev)
    away_pts = estimated_points(away_pts_base, away_off_dev, home_def_dev)

    print(f'Expected score: {home_team} {round(home_pts,1)} - {away_team} {round(away_pts,1)}')


#%%
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main(sys.argv)
