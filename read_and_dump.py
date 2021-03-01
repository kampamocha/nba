"""
Created on Thu Feb 25 22:35:08 2021
@author: Diego Campos Sobrino
"""
import getopt
import sys
import data_helpers as dh

#%%
def main(argv):
    usage = 'Usage: read_and_dump.py [-d <data_dir>]'
    data_folder = ''
    db_name = 'nba'

    try:
        opts, args = getopt.getopt(argv[1:], 'hd:', ['help', 'data_dir='])
    except getopt.GetoptError as err:
        print(err)
        print(usage)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(usage)
            sys.exit()
        elif opt in ('-d', '--data_dir'):
            data_folder = arg

    # Get MySQL credentials
    host, user, password = dh.load_mysql_credentials()

    # Create connection
    connection = dh.create_connection(host, user, password)
    if not connection:
        connection.close()
        sys.exit()

    # Create database
    cursor = connection.cursor()
    if not dh.create_database(cursor, db_name):
        cursor.close()
        connection.close()
        sys.exit()

    # Use database
    if not dh.use_database(cursor, db_name):
        cursor.close()
        connection.close()
        sys.exit()

    #cursor.execute('SET GLOBAL max_allowed_packet=268435456')
    #cursor.execute('SET GLOBAL max_allowed_packet=134217728')

    """
    TEAMS
    """
    # Read data
    teams = dh.read_data(data_folder, 'teams.csv')

    # Create table
    if not dh.create_table(cursor, 'teams', teams, pk=["TEAM_ID"]):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill table
    if not dh.insert_data(cursor, 'teams', teams):
        cursor.close()
        connection.close()
        sys.exit()

    """
    PLAYERS
    """
    # Read players data
    players_data = dh.read_data(data_folder, 'players.csv')
    players = players_data[['PLAYER_ID', 'PLAYER_NAME']].drop_duplicates()

    # Create players table
    if not dh.create_table(cursor, 'players', players, pk=['PLAYER_ID']):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill teams table
    if not dh.insert_data(cursor, 'players', players):
        cursor.close()
        connection.close()
        sys.exit()

    # Player seasons
    player_seasons = players_data[['PLAYER_ID', 'TEAM_ID', 'SEASON']].drop_duplicates()

    # Create player_seasons table
    if not dh.create_table(cursor, 'player_seasons', player_seasons,
                        pk=['PLAYER_ID', 'TEAM_ID', 'SEASON'],
                        fk=[('PLAYER_ID', 'players', 'PLAYER_ID'), ('TEAM_ID', 'teams', 'TEAM_ID')]):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill player_seasons table
    if not dh.insert_data(cursor, 'player_seasons', player_seasons):
        cursor.close()
        connection.close()
        sys.exit()

    """
    GAMES
    """
    # Read games data
    games = dh.read_data(data_folder, 'games.csv')
    # Drop duplicated columns
    games = games.drop(['HOME_TEAM_ID', 'VISITOR_TEAM_ID'], axis=1)

    # Create games table
    if not dh.create_table(cursor, 'games', games, pk=['GAME_ID'],
                        fk=[('TEAM_ID_home', 'teams', 'TEAM_ID'), ('TEAM_ID_away', 'teams', 'TEAM_ID')]):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill games table
    if not dh.insert_data(cursor, 'games', games):
        cursor.close()
        connection.close()
        sys.exit()


    """
    GAMES_DETAILS
    """
    # Read games_details data
    games_details = dh.read_data(data_folder, 'games_details.csv')
    games_players = games_details.loc[games_details['PLAYER_ID'].drop_duplicates().index, ['PLAYER_ID', 'PLAYER_NAME']]
    # Drop redundant columns
    games_details = games_details.drop(['TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_NAME'], axis=1)

    # Update players table with additional players that appear in games_details
    # NOTE: player_seasons table should be updated too.
    #       For brevity is not done. It shouldn't affect the query or model results
    if not dh.insert_data(cursor, 'players', games_players):
        cursor.close()
        connection.close()
        sys.exit()

    # Create games_details table
    if not dh.create_table(cursor, 'games_details', games_details, pk=['GAME_ID', 'PLAYER_ID'],
                        fk=[('GAME_ID', 'games', 'GAME_ID'), ('PLAYER_ID', 'players', 'PLAYER_ID'), ('TEAM_ID', 'teams', 'TEAM_ID')]):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill games_details table
    if not dh.insert_data(cursor, 'games_details', games_details):
        cursor.close()
        connection.close()
        sys.exit()


    """
    RANKING

    Ranking data could be normalizaed with several tables separating seasons,
    conferences and so on. For brevity this was not performed here,
    besides this data is not used on subsecuent queries and prediction model.

    # Read ranking data
    ranking = read_data(data_folder, 'ranking.csv')

    # Create ranking table
    if not create_table(cursor, 'ranking', ranking):
        cursor.close()
        connection.close()
        sys.exit()

    # Fill ranking table
    if not insert_data(cursor, 'ranking', ranking):
        cursor.close()
        connection.close()
        sys.exit()
    """

    #%%
    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main(sys.argv)
