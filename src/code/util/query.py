# BASE CREATION QUERIES -----------------------------
CREATE_PLAYER = """
CREATE TABLE IF NOT EXISTS player ( 
    player_id SERIAL PRIMARY KEY,
    full_name text,
    first_name text,
    last_name text
); 
CREATE INDEX idx_player_full_name 
ON player(full_name ASC);"""

CREATE_CONFERENCE = """
CREATE TABLE IF NOT EXISTS conference (
    conference_id SERIAL PRIMARY KEY,
    abbrev text,
    full_name text,
    CONSTRAINT conference_unique_abbrev UNIQUE (abbrev)
);
"""

CREATE_TEAM = """
CREATE TABLE IF NOT EXISTS team ( 
    team_id SERIAL PRIMARY KEY,
    abbrev text,
    entity_name text,
    mascot text,
    CONSTRAINT team_unique_abbrev UNIQUE (abbrev)
);

CREATE INDEX idx_team_abbrev 
ON team(abbrev ASC);"""


CREATE_TEAM_TO_CONFERENCE = """
CREATE TABLE IF NOT EXISTS team_to_conference ( 
    team_id int REFERENCES team(team_id), 
    conference_id int REFERENCES conference(conference_id),
    year int,
    PRIMARY KEY (team_id, conference_id)
); """

CREATE_PLAYER_TO_TEAM = """
CREATE TABLE IF NOT EXISTS player_to_team ( 
    player_id int REFERENCES player(player_id), 
    team_id int REFERENCES team(team_id),
    year int,
    PRIMARY KEY (player_id, team_id)
); """


# DETAIL CREATION QUERIES ---------------------------------------
CREATE_DEFENSE = """
CREATE TABLE IF NOT EXISTS defense ( 
    defense_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    interceptions int,
    int_yards int,
    int_yards_average float(2),
    int_longest int,
    touchdowns int,
    solo_tackles int,
    assisted_tackles int,
    total_tackles int,
    sacks float(1),
    sack_yards_lost float(2),
    year int,
    CONSTRAINT defense_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_PUNTING = """
CREATE TABLE IF NOT EXISTS punting ( 
    punting_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    punts int,
    yards int,
    punt_yard_average float(2),
    longest int,
    touchbacks int,
    inside_20 int,
    blocked int,
    net_yards float(2),
    returned int,
    return_yards int,
    year int,
    CONSTRAINT punting_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_PASSING = """
CREATE TABLE IF NOT EXISTS passing (
    passing_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    attempts int,
    completions int,
    completion_percentage float(1),
    yards int,
    yards_per_attempt float(1),
    touchdowns int,
    touchdown_percentage float(1),
    interceptions int,
    interception_percentage float(1),
    longest int,
    times_sacked int,
    sack_yards_lost int,
    passer_rating float(1),
    year int,
    CONSTRAINT passing_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_RECEIVING = """
CREATE TABLE IF NOT EXISTS receiving (
    receiving_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    receptions int,
    yards int,
    yards_average float(2),
    longest int,
    touchdowns int,
    year int,
    CONSTRAINT receiving_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_RUSHING = """
CREATE TABLE IF NOT EXISTS rushing (
    rushing_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    attempts int,
    gain int,
    loss int,
    net_yards int,
    avg_yards_per_carry float(2),
    longest int,
    touchdowns int,
    year int,
    CONSTRAINT rushing_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_SCORING = """
CREATE TABLE IF NOT EXISTS scoring (
    scoring_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    points int,
    total_tds int,
    running_tds int,
    passing_tds int,
    kick_returns int,
    punt_returns int,
    interception_returns int,
    fumble_returns int,
    blocked_kick_return int,
    blocked_punt_return int,
    field_goal_miss_return int,
    extra_point_made int,
    extra_point_attempted int,
    field_goals_made int,
    field_goals_attempted int,
    two_point_conversion int,
    safeties int,
    year int,
    CONSTRAINT scoring_player_team_year UNIQUE(player_id, team_id, year)  
); """

CREATE_KICKING = """
CREATE TABLE IF NOT EXISTS kicking (
    kicking_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    extra_point_made int,
    extra_point_attempted int,
    field_goals_made int,
    field_goals_attempted int,
    fg_0_to_19_made int,
    fg_0_to_19_attempted int,
    fg_20_to_29_made int,
    fg_20_to_29_attempted int,
    fg_30_to_39_made int,
    fg_30_to_39_attempted int,
    fg_40_to_49_made int,
    fg_40_to_49_attempted int,
    fg_50_plus_made int,
    fg_50_plus_attempted int,
    longest int,
    points int,
    year int,
    CONSTRAINT kicking_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_KICKOFF = """
CREATE TABLE IF NOT EXISTS kickoff (
    kickoff_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    number_of int,
    yards int,
    average_yards float(1),
    longest int,
    touchbacks int,
    out_of_bounds int,
    returned int,
    returned_yards int,
    returned_for_td int,
    year int,
    CONSTRAINT kickoff_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_KICKOFFRETURN = """
CREATE TABLE IF NOT EXISTS kickoffreturn (
    kickoffreturn_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    number_of int,
    yards int,
    average_yards float(2),
    fair_catch int,
    longest int,
    touchdowns int,
    year int,
    CONSTRAINT kickoffreturn_player_team_year UNIQUE(player_id, team_id, year)
); """

CREATE_PUNTRETURN = """
CREATE TABLE IF NOT EXISTS puntreturn (
    puntreturn_id SERIAL PRIMARY KEY,
    player_id int REFERENCES player (player_id),
    team_id int REFERENCES team (team_id),
    number_of int,
    yards int,
    average_yards float(2),
    fair_catch int,
    longest int,
    touchdowns int,
    year int,
    CONSTRAINT puntreturn_player_team_year UNIQUE(player_id, team_id, year)
); """

# BASE DROP TABLE QUERIES --------------------------------
DROP_PLAYER = "DROP TABLE IF EXISTS player"
DROP_TEAM = "DROP TABLE IF EXISTS team"
DROP_CONFERENCE = "DROP TABLE IF EXISTS conference"
DROP_PLAYER_TO_TEAM = "DROP TABLE IF EXISTS player_to_team"
DROP_TEAM_TO_CONFERENCE = "DROP TABLE IF EXISTS team_to_conference"

# DETAIL DROP TABLE QUERIES
DROP_DEFENSE = "DROP TABLE IF EXISTS defense"
DROP_RUSHING = "DROP TABLE IF EXISTS rushing"
DROP_SCORING = "DROP TABLE IF EXISTS scoring"
DROP_PASSING = "DROP TABLE IF EXISTS passing"
DROP_RECEIVING = "DROP TABLE IF EXISTS receiving"
DROP_PUNTING = "DROP TABLE IF EXISTS punting"

# DELETION QUERIES-------------------------------

DELETE_FROM_PLAYER = "DELETE FROM player"
DELETE_FROM_TEAM = "DELETE FROM team"
DELETE_FROM_DEFENSE = "DELETE FROM defense"
DELETE_FROM_PASSING = "DELETE FROM passing"
DELETE_FROM_RECEIVING = "DELETE FROM receiving"
DELETE_FROM_RUSHING = "DELETE FROM rushing"
DELETE_FROM_SCORING = "DELETE FROM scoring"

# BASE INSERTION QUERIES ----------------------
INSERT_CONFERENCE = """INSERT INTO conference (abbrev) VALUES (%s)  
                       ON CONFLICT ON CONSTRAINT conference_unique_abbrev DO NOTHING
                       RETURNING *;                    
"""

INSERT_TEAM = """INSERT INTO team (abbrev, entity_name, mascot) VALUES (%s,%s,%s)
                 ON CONFLICT ON CONSTRAINT team_unique_abbrev DO NOTHING
                 RETURNING *;
"""

INSERT_PLAYER = """INSERT INTO player (full_name, first_name, last_name) 
                                       VALUES (%s, %s, %s)
                                       RETURNING *"""

INSERT_TEAM_TO_CONFERENCE = """INSERT INTO team_to_conference (team_id, conference_id, year)
                                                               VALUES (%s, %s, %s)
                                                               ON CONFLICT DO NOTHING;
"""

INSERT_PLAYER_TO_TEAM = """INSERT INTO player_to_team (player_id, team_id, year)
                                                       VALUES (%s, %s, %s)
                                                       ON CONFLICT DO NOTHING;
"""
# DETAIL INSERTION QUERIES --------------------------
INSERT_DEFENSE = """INSERT INTO defense (player_id, 
                                         team_id, 
                                         interceptions, 
                                         int_yards, 
                                         int_yards_average, 
                                         int_longest, 
                                         touchdowns, 
                                         solo_tackles, 
                                         assisted_tackles, 
                                         total_tackles, 
                                         sacks,
                                         sack_yards_lost, 
                                         year)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         ON CONFLICT ON CONSTRAINT defense_player_team_year DO NOTHING;"""

INSERT_PUNTING = """INSERT INTO punting (player_id, 
                                         team_id, 
                                         punts, 
                                         yards, 
                                         punt_yard_average, 
                                         longest, 
                                         touchbacks, 
                                         inside_20, 
                                         blocked,
                                         net_yards, 
                                         returned,
                                         return_yards, 
                                         year)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         ON CONFLICT ON CONSTRAINT punting_player_team_year DO NOTHING;"""

INSERT_PASSING = """INSERT INTO passing (player_id,
                                         team_id,
                                         attempts,
                                         completions,
                                         completion_percentage,
                                         yards,
                                         yards_per_attempt,
                                         touchdowns,
                                         touchdown_percentage,
                                         interceptions,
                                         interception_percentage,
                                         longest,
                                         times_sacked,
                                         sack_yards_lost,
                                         passer_rating,
                                         year)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         ON CONFLICT ON CONSTRAINT passing_player_team_year DO NOTHING;"""

INSERT_RECEIVING = """INSERT INTO receiving (player_id,
                                             team_id,
                                             receptions,
                                             yards,
                                             yards_average,
                                             longest,
                                             touchdowns,
                                             year)
                                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                             ON CONFLICT ON CONSTRAINT receiving_player_team_year DO NOTHING;"""

INSERT_RUSHING = """INSERT INTO rushing (player_id,
                                         team_id,
                                         attempts,
                                         gain,
                                         loss,
                                         net_yards,
                                         avg_yards_per_carry,
                                         longest,
                                         touchdowns,
                                         year)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         ON CONFLICT ON CONSTRAINT rushing_player_team_year DO NOTHING;"""

INSERT_SCORING = """INSERT INTO scoring(player_id,
                                        team_id,
                                        points,
                                        total_tds,
                                        running_tds,
                                        passing_tds,
                                        kick_returns,
                                        punt_returns,
                                        interception_returns,
                                        fumble_returns,
                                        blocked_kick_return,
                                        blocked_punt_return,
                                        field_goal_miss_return,
                                        extra_point_made,
                                        extra_point_attempted,
                                        field_goals_made,
                                        field_goals_attempted,
                                        two_point_conversion,
                                        safeties,
                                        year)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT scoring_player_team_year DO NOTHING;"""

INSERT_KICKING = """INSERT INTO kicking(player_id,
                                        team_id,
                                        extra_point_made,
                                        extra_point_attempted,
                                        field_goals_made,
                                        field_goals_attempted,
                                        fg_0_to_19_made,
                                        fg_0_to_19_attempted,
                                        fg_20_to_29_made,
                                        fg_20_to_29_attempted,
                                        fg_30_to_39_made,
                                        fg_30_to_39_attempted,
                                        fg_40_to_49_made,
                                        fg_40_to_49_attempted,
                                        fg_50_plus_made,
                                        fg_50_plus_attempted,
                                        longest,
                                        points,
                                        year)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT kicking_player_team_year DO NOTHING;                                        
"""

INSERT_KICKOFF = """INSERT INTO kickoff (player_id,
                                         team_id,
                                         number_of,
                                         yards,
                                         average_yards,
                                         longest,
                                         touchbacks,
                                         out_of_bounds,
                                         returned,
                                         returned_yards,
                                         returned_for_td,
                                         year)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT kickoff_player_team_year DO NOTHING;
                            """

INSERT_KICKOFFRETURN = """INSERT INTO kickoffreturn (player_id,
                                                     team_id,
                                                     number_of,
                                                     yards,
                                                     average_yards,
                                                     fair_catch,
                                                     longest,
                                                     touchdowns,
                                                     year)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT kickoffreturn_player_team_year DO NOTHING;
                            """

INSERT_PUNTRETURN = """INSERT INTO puntreturn (player_id,
                                               team_id,
                                               number_of,
                                               yards,
                                               average_yards,
                                               fair_catch,
                                               longest,
                                               touchdowns,
                                               year)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT puntreturn_player_team_year DO NOTHING;
                            """
# SELECTION QUERIES ----------------------------
SELECT_CONFERENCE_BY_ABBREV = "SELECT conference_id FROM conference WHERE abbrev = %s"

SELECT_TEAM_BY_ABBREV = "SELECT team_id FROM team WHERE abbrev = %s"

SELECT_PLAYER_BY_NAME_AND_TEAM_ABBREV = """
SELECT p.player_id 
FROM player p JOIN team t 
ON p.team_id = t.team_id
WHERE p.full_name = %s
AND t.abbrev = %s
"""

SELECT_PLAYER_BY_NAME_AND_TEAM_ID = """
SELECT p.player_id 
FROM player p, player_to_team ptt
WHERE p.player_id = ptt.player_id
AND p.full_name = %s
AND ptt.team_id = %s
"""

SELECT_TABLE_BY_PLAYER_ID_AND_YEAR = """
SELECT 1
FROM {table} t
WHERE t.player_id = %s
AND t.year = %s
"""