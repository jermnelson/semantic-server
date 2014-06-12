#-------------------------------------------------------------------------------
# Name:        helpers
# Purpose:     Module provides functional helpers for Semantic Server's iPython
#              notebooks.
#
# Author:      Jeremy Nelson
#
# Created:     2014/03/06
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import redis

def get_month_stats(stat, year, month, redis_ds=redis.StrictRedis()):
    month_stats = []
    for i in xrange(1, 32):
        try:
            day = datetime.datetime(year, month, i)
            redis_key = day.strftime("%Y-%m-%d:{}".format(stat))
            month_stats.append((day, redis_ds.bitcount(redis_key)))
        except ValueError:
            break
    return month_stats

def main():
    pass

if __name__ == '__main__':
    main()
