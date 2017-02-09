# 
# An example of processing data which generates a json file containing
# login times sorted by servers which are then sorted by week.
# 
# Usage: python3 processdata.py <sqlitedb> <outfile>
# 
# Example Output:
# {
#     "2016-01-04 00:00:00"" {
#         "server.example.com": ["2016-01-05 12:05:00", "2016-01-07 09:00:03"]
#     }
# }
#


import calendar
import collections
from datetime import datetime, timedelta
import json
import sqlite3
import sys

from logusers import Record, RECORD_TABLE


QUERY_RECORDS = 'SELECT * FROM %s' % (RECORD_TABLE)


def get_week(when):
    """
    Determine the date of 12:00 am on Monday of the week for the given object.
    """
    day = when.day - when.weekday()
    month = when.month
    if day <= 0:
        month -= 1
        if month < 0:
            month += 12
        day += calendar.monthrange(when.year, month)[1] - 1
    try:
        return datetime(when.year, month, day)
    except ValueError as e:
        print(month, day)


def main(dbfile, datafile):
    # Open the database
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    rows = c.execute(QUERY_RECORDS)

    # dict of date objects specifying the first sunday of a week to a dict
    # of server name to list of Records which happened in that week on that
    # server
    by_week_server = collections.defaultdict(lambda: collections.defaultdict(list))
    for tup in rows:
        rec = Record.from_tuple(tup)  # Create a record object
        week = get_week(rec.login)    # Get the week it's from
        # The value is a list [weekday, hour]
        value = str(rec.login)   # use rec.login for the full date
        by_week_server[str(week)][rec.hostname].append(value) # Now add it to the pile

    with open(datafile, "w") as fd:
        json.dump(by_week_server, fd)

    # We're done, close the database
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
