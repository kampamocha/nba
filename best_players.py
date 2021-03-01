"""
Created on Thu Feb 25 22:35:08 2021
@author: Diego Campos Sobrino
"""
import getopt
import sys
import data_helpers as dh

#%%
def main(argv):
    usage = 'Usage: best_players.py -s <season>'
    db_name = 'nba'
    season = 2020

    try:
        opts, args = getopt.getopt(argv[1:], 'hs:', ['help', 'season='])
    except getopt.GetoptError as err:
        print(err)
        print(usage)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(usage)
            sys.exit()
        elif opt in ('-s', '--season'):
            season = int(arg)

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
    query = """
    WITH weekly_prod AS (
    	SELECT *,
    	RANK() OVER
    	(
    		PARTITION BY SEASON_WEEK
    		ORDER BY PRODUCTIVITY DESC
    	) prod_rank
    	FROM
    	(
    		SELECT YEARWEEK(g.GAME_DATE_EST) AS SEASON_WEEK, p.PLAYER_NAME, sum(PTS + REB + AST) AS PRODUCTIVITY
    		FROM games_details AS gd
    		JOIN games AS g ON gd.GAME_ID = g.GAME_ID
    		JOIN players AS p ON gd.PLAYER_ID = p.PLAYER_ID
    		WHERE g.SEASON = %s
    		GROUP BY SEASON_WEEK, gd.PLAYER_ID
    	) prod
    )
    SELECT SEASON_WEEK, PLAYER_NAME, PRODUCTIVITY
    FROM weekly_prod
    WHERE prod_rank <= 1;
    """

    cursor.execute(query, (season,))

    print('{:<10} {:<30} {:<5}'.format('WEEK','PLAYER','PROD'))
    for (week, player, productivity) in cursor:
        print('{:<10} {:<30} {:<5}'.format(week, player, productivity))


#%%
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main(sys.argv)
