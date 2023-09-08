"""
 A function that uploads raw database info into modeled postgres db
"""
import psycopg2 as p
import boto3
from enum import Enum
import util.query as q
import time
import os
from dotenv import load_dotenv
import csv
import argparse

# global variables
matrix_team_names = []


class StatType(Enum):
    PUNTING = 0
    KICKING = 1
    KICKOFF = 2
    KICKOFFRETURN = 3
    PASSING = 4
    PUNTRETURN = 5
    RECEIVING = 6
    RUSHING = 7
    SCORING = 8
    DEFENSE = 9


class DefenseColumn(Enum):
    PLAYER = 0
    TEAM = 1
    INTERCEPTIONS = 2
    YARDS = 3
    AVG_INT_RETURN_YARDS = 4
    LONGEST_INT = 5
    TOUCHDOWNS = 6
    SOLO_TACKLES = 7
    ASSISTED_TACKLES = 8
    TOTAL_TACKLES = 9
    SACKS = 10
    SACK_YARDS_LOST = 11
    STAT_TYPE = 12
    CONFERENCE = 13
    YEAR = 14


class PuntingColumn(Enum):
    PLAYER = 0
    TEAM = 1
    PUNTS = 2
    YARDS = 3
    AVERAGE_YARDS = 4
    LONGEST = 5
    TOUCHBACKS = 6
    INSIDE_20 = 7
    BLOCKED = 8
    NET_YARDS = 9
    RETURNED = 10
    RETURN_YARDS = 11
    STAT_TYPE = 12
    CONFERENCE = 13
    YEAR = 14


class ReceivingColumn(Enum):
    PLAYER = 0
    TEAM = 1
    RECEPTIONS = 2
    YARDS = 3
    AVERAGE_YARDS = 4
    LONGEST = 5
    TOUCHDOWNS = 6
    STAT_TYPE = 7
    CONFERENCE = 8
    YEAR = 9


class RushingColumn(Enum):
    PLAYER = 0
    TEAM = 1
    ATTEMPTS = 2
    GAIN = 3
    LOSS = 4
    NET_YARDS = 5
    AVG_YARDS_PER_CARRY = 6
    LONGEST = 7
    TOUCHDOWNS = 8
    STAT_TYPE = 9
    CONFERENCE = 10
    YEAR = 11


class ScoringColumn(Enum):
    PLAYER = 0
    TEAM = 1
    POINTS = 2
    TOTAL_TDS = 3
    RUNNING_TDS = 4
    PASSING_TDS = 5
    KICK_RETURNS = 6
    PUNT_RETURNS = 7
    INTERCEPTION_RETURNS = 8
    FUMBLE_RETURNS = 9
    BLOCKED_KICK_RETURN = 10
    BLOCKED_PUNT_RETURN = 11
    FIELD_GOAL_MISS_RETURN = 12
    PAT_PERCENTAGE = 13
    FG_PERCENTAGE = 14
    TWO_POINT_CONVERSION = 15
    SAFETIES = 16
    STAT_TYPE = 17
    CONFERENCE = 18
    YEAR = 19


class PassingColumn(Enum):
    PLAYER = 0
    TEAM = 1
    ATTEMPTS = 2
    COMPLETIONS = 3
    COMPLETION_PERCENTAGE = 4
    YARDS = 5
    YARDS_PER_ATTEMPT = 6
    TOUCHDOWNS = 7
    TOUCHDOWN_PERCENTAGE = 8
    INTERCEPTIONS = 9
    INTERCEPTION_PERCENTAGE = 10
    LONGEST = 11
    TIMES_SACKED = 12
    SACK_YARDS_LOST = 13
    PASSER_RATING = 14
    STAT_TYPE = 15
    CONFERENCE = 16
    YEAR = 17


class KickingColumn(Enum):
    PLAYER = 0
    TEAM = 1
    PAT_PERCENTAGE = 2
    FG_PERCENTAGE = 3
    FG_0_TO_19_PERCENTAGE = 4
    FG_20_TO_29_PERCENTAGE = 5
    FG_30_TO_39_PERCENTAGE = 6
    FG_40_TO_49_PERCENTAGE = 7
    FG_50_PLUS_PERCENTAGE = 8
    LONGEST = 9
    POINTS = 10
    STAT_TYPE = 11
    CONFERENCE = 12
    YEAR = 13


class KickoffColumn(Enum):
    PLAYER = 0
    TEAM = 1
    NUMBER_OF = 2
    YARDS = 3
    AVERAGE = 4
    LONGEST = 5
    TOUCHBACKS = 6
    OUT_OF_BOUNDS = 7
    RETURNED = 8
    RETURNED_YARDS = 9
    RETURNED_FOR_TD = 10
    STAT_TYPE = 11
    CONFERENCE = 12
    YEAR = 13


class KickoffReturnColumn(Enum):
    PLAYER = 0
    TEAM = 1
    NUMBER_OF = 2
    YARDS = 3
    AVERAGE = 4
    FAIR_CATCH = 5
    LONGEST = 6
    TOUCHDOWNS = 7
    STAT_TYPE = 8
    CONFERENCE = 9
    YEAR = 10


class PuntReturnColumn(Enum):
    PLAYER = 0
    TEAM = 1
    NUMBER_OF = 2
    YARDS = 3
    AVERAGE = 4
    FAIR_CATCH = 5
    LONGEST = 6
    TOUCHDOWNS = 7
    STAT_TYPE = 8
    CONFERENCE = 9
    YEAR = 10


def to_int(value):
    # remove t indicator (used if there is a tie on values that are max/min columns i.e longest)
    # max columns also use "--" as 0 since there would be no valid max/min if the
    # associated column is zero. IsNumeric will check if the number is numeric after removing the tie
    # if it is not we will assume the value is zero
    if value[-1] == "t":
        value = value[:-1]
    value = value.replace(',', '')

    try:
        return int(value)
    except ValueError:
        return int(0)


def to_float(value):
    if value[-1] == "t":
        value = value[:-1]
    value = value.replace(',', '')

    try:
        return float(value)
    except ValueError:
        return float(0)


def split_percentage(value):
    value_list = value.split('/')

    if len(value_list) == 1:
        value_list.append('0')

    return value_list


def load_team_names():
    with open('../files/team_full_names.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            # 'row' is a list representing a row of CSV data
            matrix_team_names.append(row)


def get_team_names(abbrev):
    for row in range(len(matrix_team_names)):
        if matrix_team_names[row][0] == abbrev:
            return matrix_team_names[row]
    return None;


def load_base_tables(cur, player, team, conference, year):
    # CONFERENCE
    # Each conference likely inserted from first stat file
    # so try SELECT before INSERT to reduce DB calls

    # some error checks
    if to_int(conference) != 0:
        raise Exception("Conference is numeric. Player" + player)
    if conference == 0:
        raise Exception("Conference is 0. Player" + player)

    if to_int(team) != 0:
        raise Exception("Team is numeric. Player " + player)
    if team == 0:
        raise Exception("Team is 0. Player" + player)

    cur.execute(q.SELECT_CONFERENCE_BY_ABBREV, (conference,))

    row_conference = cur.fetchall()
    if len(row_conference) == 0:

        cur.execute(q.INSERT_CONFERENCE, (conference,))

        row_conference = cur.fetchall()

        if len(row_conference) == 0:
            raise Exception("no rows inserted into conference")
        elif len(row_conference) > 1:
            raise Exception("too many rows inserted into conference, rows: " + len(row_conference))

        conference_id = row_conference[0][0]
    elif len(row_conference) == 1:
        conference_id = row_conference[0][0]
    else:
        raise Exception("too many conference rows retrieved, rows: " + len(row_conference))

    # TEAM
    # Each conference likely inserted from first stat file
    # so try SELECT before INSERT to reduce DB calls
    cur.execute(q.SELECT_TEAM_BY_ABBREV, (team,))

    row_team = cur.fetchall()
    if len(row_team) == 0:

        matrix_team = get_team_names(team)
        entity_name = matrix_team[1]
        mascot = matrix_team[2]

        cur.execute(q.INSERT_TEAM, (team, entity_name, mascot))

        row_team = cur.fetchall()
        if len(row_team) == 0:
            raise Exception("no rows returned from team")
        elif len(row_team) > 1:
            raise Exception("too many rows returned from team, rows: " + len(row_team))

        team_id = row_team[0][0]
    elif len(row_team) == 1:
        team_id = row_team[0][0]
    else:
        raise Exception("too many team rows inserted, rows: " + len(row_team))

    # TEAM TO CONFERENCE
    # always run since teams could switch conferences
    # does nothing if record already exists
    cur.execute(q.INSERT_TEAM_TO_CONFERENCE, (team_id, conference_id, year))

    # PLAYERs
    # Check if player exists before attempting INSERT
    cur.execute(q.SELECT_PLAYER_BY_NAME_AND_TEAM_ID, (player, team_id))

    row_player = cur.fetchall()
    if len(row_player) == 0:

        name_array = player.split(" ", 1)
        first_name = name_array[0]
        last_name = name_array[1]

        cur.execute(q.INSERT_PLAYER, (player, first_name, last_name))
        row_player = cur.fetchall()

        if row_player is None:
            raise Exception("no rows returned from player")
        elif len(row_player) > 1:
            raise Exception("too many rows returned from player, rows: " + len(row_player))

        player_id = row_player[0][0]
    elif len(row_player) == 1:
        player_id = row_player[0][0]
    else:
        raise Exception("too many rows inserted into player, rows: " + len(row_player))

    # PLAYER TO TEAM
    # always run since players could switch teams
    # does nothing if record already exists
    cur.execute(q.INSERT_PLAYER_TO_TEAM, (player_id, team_id, year))

    return player_id, team_id, conference_id


def load_defense(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][DefenseColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][DefenseColumn.TEAM.value]['VarCharValue']
            interceptions = row['Data'][DefenseColumn.INTERCEPTIONS.value]['VarCharValue']
            int_yards = row['Data'][DefenseColumn.YARDS.value]['VarCharValue']
            avg_int_return_yards = to_float(row['Data'][DefenseColumn.AVG_INT_RETURN_YARDS.value]['VarCharValue'])
            longest_int = to_int(row['Data'][DefenseColumn.LONGEST_INT.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][DefenseColumn.TOUCHDOWNS.value]['VarCharValue'])
            solo_tackles = to_int(row['Data'][DefenseColumn.SOLO_TACKLES.value]['VarCharValue'])
            assisted_tackles = to_int(row['Data'][DefenseColumn.ASSISTED_TACKLES.value]['VarCharValue'])
            total_tackles = to_int(row['Data'][DefenseColumn.TOTAL_TACKLES.value]['VarCharValue'])
            sacks = to_int(row['Data'][DefenseColumn.SACKS.value]['VarCharValue'])
            sack_yards_lost = to_int(row['Data'][DefenseColumn.SACK_YARDS_LOST.value]['VarCharValue'])
            conference = row['Data'][DefenseColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][DefenseColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]
            cur.execute(q.INSERT_DEFENSE, (player_id,
                                           team_id,
                                           interceptions,
                                           int_yards,
                                           avg_int_return_yards,
                                           longest_int,
                                           touchdowns,
                                           solo_tackles,
                                           assisted_tackles,
                                           total_tackles,
                                           sacks,
                                           sack_yards_lost,
                                           year))


def load_punting(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][PuntingColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][PuntingColumn.TEAM.value]['VarCharValue']
            punts = to_int(row['Data'][PuntingColumn.PUNTS.value]['VarCharValue'])
            yards = to_int(row['Data'][PuntingColumn.YARDS.value]['VarCharValue'])
            average_yards = to_float(row['Data'][PuntingColumn.AVERAGE_YARDS.value]['VarCharValue'])
            longest = to_int(row['Data'][PuntingColumn.LONGEST.value]['VarCharValue'])
            touchbacks = to_int(row['Data'][PuntingColumn.TOUCHBACKS.value]['VarCharValue'])
            inside_20 = to_int(row['Data'][PuntingColumn.INSIDE_20.value]['VarCharValue'])
            net_yards = to_float(row['Data'][PuntingColumn.NET_YARDS.value]['VarCharValue'])
            blocked = to_int(row['Data'][PuntingColumn.BLOCKED.value]['VarCharValue'])
            returned = to_int(row['Data'][PuntingColumn.RETURNED.value]['VarCharValue'])
            return_yards = to_int(row['Data'][PuntingColumn.RETURN_YARDS.value]['VarCharValue'])
            conference = row['Data'][PuntingColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][PuntingColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_PUNTING, (player_id,
                                           team_id,
                                           punts,
                                           yards,
                                           average_yards,
                                           longest,
                                           touchbacks,
                                           inside_20,
                                           blocked,
                                           net_yards,
                                           returned,
                                           return_yards,
                                           year))


def load_receiving(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][ReceivingColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][ReceivingColumn.TEAM.value]['VarCharValue']
            receptions = to_int(row['Data'][ReceivingColumn.RECEPTIONS.value]['VarCharValue'])
            yards = to_int(row['Data'][ReceivingColumn.YARDS.value]['VarCharValue'])
            average_yards = to_float(row['Data'][ReceivingColumn.AVERAGE_YARDS.value]['VarCharValue'])
            longest = to_int(row['Data'][ReceivingColumn.LONGEST.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][ReceivingColumn.TOUCHDOWNS.value]['VarCharValue'])
            conference = row['Data'][ReceivingColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][ReceivingColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_RECEIVING, (player_id,
                                             team_id,
                                             receptions,
                                             yards,
                                             average_yards,
                                             longest,
                                             touchdowns,
                                             year))


def load_rushing(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][RushingColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][RushingColumn.TEAM.value]['VarCharValue']
            attempts = to_int(row['Data'][RushingColumn.ATTEMPTS.value]['VarCharValue'])
            gain = to_int(row['Data'][RushingColumn.GAIN.value]['VarCharValue'])
            loss = to_int(row['Data'][RushingColumn.LOSS.value]['VarCharValue'])
            net_yards = to_int(row['Data'][RushingColumn.NET_YARDS.value]['VarCharValue'])
            avg_yards_per_carry = to_float(row['Data'][RushingColumn.AVG_YARDS_PER_CARRY.value]['VarCharValue'])
            longest = to_int(row['Data'][RushingColumn.LONGEST.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][RushingColumn.TOUCHDOWNS.value]['VarCharValue'])
            conference = row['Data'][RushingColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][RushingColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_RUSHING, (player_id,
                                           team_id,
                                           attempts,
                                           gain,
                                           loss,
                                           net_yards,
                                           avg_yards_per_carry,
                                           longest,
                                           touchdowns,
                                           year))


def load_kickoff(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][KickoffColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][KickoffColumn.TEAM.value]['VarCharValue']
            number_of = to_int(row['Data'][KickoffColumn.NUMBER_OF.value]['VarCharValue'])
            yards = to_int(row['Data'][KickoffColumn.YARDS.value]['VarCharValue'])
            average = to_float(row['Data'][KickoffColumn.AVERAGE.value]['VarCharValue'])
            longest = to_int(row['Data'][KickoffColumn.LONGEST.value]['VarCharValue'])
            touchbacks = to_int(row['Data'][KickoffColumn.TOUCHBACKS.value]['VarCharValue'])
            out_of_bounds = to_int(row['Data'][KickoffColumn.OUT_OF_BOUNDS.value]['VarCharValue'])
            returned = to_int(row['Data'][KickoffColumn.RETURNED.value]['VarCharValue'])
            returned_yards = to_int(row['Data'][KickoffColumn.RETURNED_YARDS.value]['VarCharValue'])
            returned_for_td = row['Data'][KickoffColumn.RETURNED_FOR_TD.value]['VarCharValue']
            conference = row['Data'][KickoffColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][KickoffColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_KICKOFF, (player_id,
                                           team_id,
                                           number_of,
                                           yards,
                                           average,
                                           longest,
                                           touchbacks,
                                           out_of_bounds,
                                           returned,
                                           returned_yards,
                                           returned_for_td,
                                           year))


def load_kickoffreturn(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][KickoffColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][KickoffReturnColumn.TEAM.value]['VarCharValue']
            number_of = to_int(row['Data'][KickoffReturnColumn.NUMBER_OF.value]['VarCharValue'])
            yards = to_int(row['Data'][KickoffReturnColumn.YARDS.value]['VarCharValue'])
            average = to_float(row['Data'][KickoffReturnColumn.AVERAGE.value]['VarCharValue'])
            fair_catch = to_int(row['Data'][KickoffReturnColumn.FAIR_CATCH.value]['VarCharValue'])
            longest = to_int(row['Data'][KickoffReturnColumn.LONGEST.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][KickoffReturnColumn.TOUCHDOWNS.value]['VarCharValue'])
            conference = row['Data'][KickoffReturnColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][KickoffReturnColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_KICKOFFRETURN, (player_id,
                                                 team_id,
                                                 number_of,
                                                 yards,
                                                 average,
                                                 fair_catch,
                                                 longest,
                                                 touchdowns,
                                                 year))


def load_puntreturn(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][KickoffColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][PuntReturnColumn.TEAM.value]['VarCharValue']
            number_of = to_int(row['Data'][PuntReturnColumn.NUMBER_OF.value]['VarCharValue'])
            yards = to_int(row['Data'][PuntReturnColumn.YARDS.value]['VarCharValue'])
            average = to_float(row['Data'][PuntReturnColumn.AVERAGE.value]['VarCharValue'])
            fair_catch = to_int(row['Data'][PuntReturnColumn.FAIR_CATCH.value]['VarCharValue'])
            longest = to_int(row['Data'][PuntReturnColumn.LONGEST.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][PuntReturnColumn.TOUCHDOWNS.value]['VarCharValue'])
            conference = row['Data'][PuntReturnColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][PuntReturnColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_PUNTRETURN, (player_id,
                                              team_id,
                                              number_of,
                                              yards,
                                              average,
                                              fair_catch,
                                              longest,
                                              touchdowns,
                                              year))


def load_scoring(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][ScoringColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][ScoringColumn.TEAM.value]['VarCharValue']
            points = to_int(row['Data'][ScoringColumn.POINTS.value]['VarCharValue'])
            total_tds = to_int(row['Data'][ScoringColumn.TOTAL_TDS.value]['VarCharValue'])
            running_tds = to_int(row['Data'][ScoringColumn.RUNNING_TDS.value]['VarCharValue'])
            passing_tds = to_int(row['Data'][ScoringColumn.PASSING_TDS.value]['VarCharValue'])
            kick_returns = to_int(row['Data'][ScoringColumn.KICK_RETURNS.value]['VarCharValue'])
            punt_returns = to_int(row['Data'][ScoringColumn.PUNT_RETURNS.value]['VarCharValue'])
            interception_returns = to_int(row['Data'][ScoringColumn.INTERCEPTION_RETURNS.value]['VarCharValue'])
            fumble_returns = to_int(row['Data'][ScoringColumn.FUMBLE_RETURNS.value]['VarCharValue'])
            blocked_kick_return = to_int(row['Data'][ScoringColumn.BLOCKED_KICK_RETURN.value]['VarCharValue'])
            blocked_punt_return = to_int(row['Data'][ScoringColumn.BLOCKED_PUNT_RETURN.value]['VarCharValue'])
            field_goal_miss_return = to_int(row['Data'][ScoringColumn.FIELD_GOAL_MISS_RETURN.value]['VarCharValue'])

            # split the percentage fields
            pat_percentage = row['Data'][ScoringColumn.PAT_PERCENTAGE.value]['VarCharValue']

            a_list = split_percentage(pat_percentage)

            extra_point_made = to_int(a_list[0])
            extra_point_attempts = to_int(a_list[1])

            fg_percentage = row['Data'][ScoringColumn.FG_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_percentage)

            fg_made = to_int(a_list[0])
            fg_attempts = to_int(a_list[1])

            safeties = to_int(row['Data'][ScoringColumn.SAFETIES.value]['VarCharValue'])
            two_point_conversion = to_int(row['Data'][ScoringColumn.TWO_POINT_CONVERSION.value]['VarCharValue'])
            conference = row['Data'][ScoringColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][ScoringColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_SCORING, (player_id,
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
                                           extra_point_attempts,
                                           fg_made,
                                           fg_attempts,
                                           two_point_conversion,
                                           safeties,
                                           year))


def load_passing(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][PassingColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][PassingColumn.TEAM.value]['VarCharValue']
            attempts = to_int(row['Data'][PassingColumn.ATTEMPTS.value]['VarCharValue'])
            completions = to_int(row['Data'][PassingColumn.COMPLETIONS.value]['VarCharValue'])
            completion_percentage = to_float(row['Data'][PassingColumn.COMPLETION_PERCENTAGE.value]['VarCharValue'])
            yards = to_int(row['Data'][PassingColumn.YARDS.value]['VarCharValue'])
            yards_per_attempt = to_float(row['Data'][PassingColumn.YARDS_PER_ATTEMPT.value]['VarCharValue'])
            touchdowns = to_int(row['Data'][PassingColumn.TOUCHDOWNS.value]['VarCharValue'])
            touchdown_percentage = to_float(row['Data'][PassingColumn.TOUCHDOWN_PERCENTAGE.value]['VarCharValue'])
            interceptions = to_int(row['Data'][PassingColumn.INTERCEPTIONS.value]['VarCharValue'])
            interception_percentage = to_float(row['Data'][PassingColumn.INTERCEPTION_PERCENTAGE.value]['VarCharValue'])
            longest = to_int(row['Data'][PassingColumn.LONGEST.value]['VarCharValue'])
            sack = to_int(row['Data'][PassingColumn.TIMES_SACKED.value]['VarCharValue'])
            yards_lost = to_int(row['Data'][PassingColumn.SACK_YARDS_LOST.value]['VarCharValue'])
            passer_rating = to_float(row['Data'][PassingColumn.PASSER_RATING.value]['VarCharValue'])
            conference = row['Data'][PassingColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][PassingColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_PASSING, (player_id,
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
                                           sack,
                                           yards_lost,
                                           passer_rating,
                                           year))


def load_kicking(cur, rows):
    for row in rows:

        # ignore first row since it is column headers
        player = row['Data'][KickingColumn.PLAYER.value]['VarCharValue']
        if player != 'player':
            team_abbrev = row['Data'][KickingColumn.TEAM.value]['VarCharValue']

            pat_percentage = row['Data'][KickingColumn.PAT_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(pat_percentage)
            extra_point_made = to_int(a_list[0])
            extra_point_attempted = to_int(a_list[1])

            fg_percentage = row['Data'][KickingColumn.FG_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_percentage)
            field_goals_made = to_int(a_list[0])
            field_goals_attempted = to_int(a_list[1])

            fg_10_to_19_percentage = row['Data'][KickingColumn.FG_0_TO_19_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_10_to_19_percentage)
            fg_10_to_19_made = to_int(a_list[0])
            fg_10_to_19_attempted = to_int(a_list[1])

            fg_20_to_29_percentage = row['Data'][KickingColumn.FG_20_TO_29_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_20_to_29_percentage)
            fg_20_to_29_made = to_int(a_list[0])
            fg_20_to_29_attempted = to_int(a_list[1])

            fg_30_to_39_percentage = row['Data'][KickingColumn.FG_30_TO_39_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_30_to_39_percentage)
            fg_30_to_39_made = to_int(a_list[0])
            fg_30_to_39_attempted = to_int(a_list[1])

            fg_40_to_49_percentage = row['Data'][KickingColumn.FG_40_TO_49_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_40_to_49_percentage)
            fg_40_to_49_made = to_int(a_list[0])
            fg_40_to_49_attempted = to_int(a_list[1])

            fg_50_plus_percentage = row['Data'][KickingColumn.FG_50_PLUS_PERCENTAGE.value]['VarCharValue']
            a_list = split_percentage(fg_50_plus_percentage)
            fg_50_plus_made = to_int(a_list[0])
            fg_50_plus_attempted = to_int(a_list[1])

            longest = to_int(row['Data'][KickingColumn.LONGEST.value]['VarCharValue'])
            points = to_int(row['Data'][KickingColumn.POINTS.value]['VarCharValue'])
            conference = row['Data'][KickingColumn.CONFERENCE.value]['VarCharValue']
            year = to_int(row['Data'][KickingColumn.YEAR.value]['VarCharValue'])

            ids = load_base_tables(cur, player, team_abbrev, conference, year)

            player_id = ids[0]
            team_id = ids[1]

            cur.execute(q.INSERT_KICKING, (player_id,
                                           team_id,
                                           extra_point_made,
                                           extra_point_attempted,
                                           field_goals_made,
                                           field_goals_attempted,
                                           fg_10_to_19_made,
                                           fg_10_to_19_attempted,
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
                                           year))


def upload_postgres(event):
    # load team name decodes
    load_team_names()

    # get environment variables
    load_dotenv(os.path.abspath('../../.env'))

    # Access environment variables using os.environ
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # Create an Athena client
    athena_client = boto3.client('athena',
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key)

    # Set the name of the S3 bucket where the query results will be stored
    s3_bucket_query_results = os.environ['S3_BUCKET_QUERY_RESULTS']

    # Get the raw database and appropriate table
    stat_type = event['stat_type']
    raw_database_name = os.environ['ATHENA_RAW_DATABASE_NAME']
    table_name = 'ss-' + stat_type

    # Set the query string
    athena_query_string = f"SELECT * FROM \"{raw_database_name}\".\"{table_name}\""

    # Execute the query
    start_time = time.time()

    response = athena_client.start_query_execution(
        QueryString=athena_query_string,
        ResultConfiguration={
            'OutputLocation': f's3://{s3_bucket_query_results}/postgres-py/'
        }
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Wait for the query to complete
    query_status = 'QUEUED'
    while query_status == 'QUEUED' or query_status == 'RUNNING':
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = response['QueryExecution']['Status']['State']

    if query_status != 'SUCCEEDED':
        raise Exception('Query not complete, status: ' + query_status)

    # Get the query results
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Get the initial NextToken
    try:
        next_token = response['NextToken']
    except KeyError:
        next_token = None

    # Connect to the Postgres database
    conn = p.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['POSTGRES_PROD_DATABASE']
    )
    # Open a cursor
    cur = conn.cursor()

    do_loop = True
    row_count = 1
    while do_loop:

        if next_token is None:
            do_loop = False
            print(f"--- LAST {stat_type} PARTITION COMING NEXT ---")

        print(f"--- Start {stat_type} Partition #{str(row_count)} ---")
        part_start = time.time()

        # Get the list of rows from the query results
        rows = response['ResultSet']['Rows']

        if stat_type == StatType.DEFENSE.name or stat_type == StatType.DEFENSE.value:
            load_defense(cur, rows)
        elif stat_type.upper() == StatType.PUNTING.name or stat_type == StatType.PUNTING.value:
            load_punting(cur, rows)
        elif stat_type.upper() == StatType.RECEIVING.name or stat_type == StatType.RECEIVING.value:
            load_receiving(cur, rows)
        elif stat_type.upper() == StatType.RUSHING.name or stat_type == StatType.RUSHING.value:
            load_rushing(cur, rows)
        elif stat_type.upper() == StatType.SCORING.name or stat_type == StatType.SCORING.value:
            load_scoring(cur, rows)
        elif stat_type.upper() == StatType.PASSING.name or stat_type == StatType.PASSING.value:
            load_passing(cur, rows)
        elif stat_type.upper() == StatType.KICKING.name or stat_type == StatType.KICKING.value:
            load_kicking(cur, rows)
        elif stat_type.upper() == StatType.KICKOFF.name or stat_type == StatType.KICKOFF.value:
            load_kickoff(cur, rows)
        elif stat_type.upper() == StatType.KICKOFFRETURN.name or stat_type == StatType.KICKOFFRETURN.value:
            load_kickoffreturn(cur, rows)
        elif stat_type.upper() == StatType.PUNTRETURN.name or stat_type == StatType.PUNTRETURN.value:
            load_puntreturn(cur, rows)

        if next_token is not None:
            # Get the next page
            response = athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token
            )

            # Get the next NextToken
            try:
                next_token = response['NextToken']
            except KeyError:
                next_token = None

        end_part = (time.time() - part_start)
        print(f"--- End Partition #{row_count}. Time:{end_part: .2f} seconds ---")
        row_count = row_count + 1

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

    end_time = (time.time() - start_time)
    print(f"--- {end_time: .2f} seconds ---")

    return {
        'message': 'upload complete'
    }


def load_all_tables():
    events = ('PUNTING',
              'KICKING',
              'KICKOFF',
              'KICKOFFRETURN',
              'PASSING',
              'PUNTRETURN',
              'RECEIVING',
              'RUSHING',
              'SCORING',
              'DEFENSE')

    for i in events:
        my_event = {
            'stat_type': i
        }

        upload_postgres(my_event)

    print('Upload to Postgres complete')


def load_one_table(table):
    my_event = {
        'stat_type': table
    }
    upload_postgres(my_event)

    print(f'Upload of table {table} to Postgres is complete')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='upload_postgres',
        description='Load S3 data into Postgres')
    parser.add_argument('-a', '--all', help="load all tables", action="store_true")
    parser.add_argument('-t', '--table', help="name of database table to load")

    args = parser.parse_args()
    if args.all:
        load_all_tables()
    elif args.table is not None:
        load_one_table(args.table)
