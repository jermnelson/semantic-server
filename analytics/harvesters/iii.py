#-------------------------------------------------------------------------------
# Name:        iii
# Purpose:     Catalog Pull Platform harvests usage information from a III
#              Millennium ILS through XML and stores result in a Redis datastore
#
#
# Author:      Jeremy Nelson
#
# Created:     2014/05/30
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------

import datetime
import pymarc
import re
import redis
import urllib2
import xml.etree.ElementTree as etree

DATE_RE = re.compile(r"""
(?P<month>\d{2,2})-
(?P<day>\d{2,2})-
(?P<year>\d{2,4})
\s*
(?P<hour>\d{2,2})*
:*
(?P<minute>\d{2,2})*
""", re.VERBOSE)

III_ITEM_URL = 'http://tiger.coloradocollege.edu/xrecord='
REDIS_DS = redis.StrictRedis()
ITEM_VAL_XPATH = "./TYPEINFO/ITEM/FIXFLD[FIXLABEL='{}']/FIXVALUE"

def add_activity(item_key, activity, value):
    REDIS_DS.zadd("item-{}".format(activity),
                  int(value), item_key)
    REDIS_DS.hset(item_key, activity, int(value))

def add_acquisition(raw_date, item_key):
    if len(raw_date.strip()) < 8:
        return
    date = add_date(raw_date, item_key, 'created')
    REDIS_DS.hset(item_key, 'created', date.isoformat())


def add_date(raw_date, item_key, redis_suffix):
    date_result = DATE_RE.search(raw_date)
    if date_result is None:
        return
    date_group = date_result.groupdict()
    if len(date_group['year']) == 2:
        date_group['year'] = int('19{}'.format( # My very own y2k bug
            date_group['year']))
    redis_offset = int(item_key.split(":")[-1])
    date_info = {}
    for name, value in date_group.items():
        if value is not None:
            date_info[name] = int(value)
    date = datetime.datetime(**date_info)
    # Year, Month, and Day
    REDIS_DS.setbit("{}:{}".format(date.year, redis_suffix),
                    redis_offset,
                    1)
    REDIS_DS.setbit(date.strftime("%Y-%m:{}".format(redis_suffix)),
                    redis_offset,
                    1)
    REDIS_DS.setbit(date.strftime("%Y-%m-%d:{}".format(redis_suffix)),
                    redis_offset,
                    1)
    return date


def add_checkout(raw_date, item_key):
    if len(raw_date.strip()) < 8:
        return
    checked_out = add_date(raw_date, item_key, 'checkouts')
    REDIS_DS.sadd("{}:checkouts".format(item_key),
                  checked_out.isoformat())

def process_marc(record):
    if type(record) == pymarc.Record:
        field = record['945']
        if field is None:
            return
        if 'y' in field.subfields:
            raw_item = field['y']
        else:
            return
    elif type(record) == dict:
        raw_item = record.get('fields', {}).get('946', None)
        if raw_item is None:
            return
    item_id = raw_item[1:-1]
    item_key = set_statistics(item_id)
    REDIS_DS.hset(item_key, 'number', raw_item)


def set_statistics(item_id):
    """Function takes an item id, queries Redis datastore"""
    if not REDIS_DS:
        return
    item_key = REDIS_DS.hget('items', item_id)
    if not item_key:
        item_key = 'iii-item:{}'.format(REDIS_DS.incr('iii-item'))
        REDIS_DS.hset('items', item_id, item_key)
    item_xml = etree.parse(urllib2.urlopen("{}{}".format(III_ITEM_URL,
                                                         item_id)))
    # Sets acquisition date
    raw_date = item_xml.find("./RECORDINFO/CREATEDATE")
    if raw_date is not None:
        add_acquisition(raw_date.text, item_key)
    # Sets last checkout
    raw_date = item_xml.find(ITEM_VAL_XPATH.format('LOUTDATE'))
    if raw_date is not None:
        add_checkout(raw_date.text, item_key)
    # Sets total checkouts
    total_checkouts = item_xml.find(ITEM_VAL_XPATH.format('TOT CHKOUT'))
    if total_checkouts is not None:
        add_activity(item_key, 'checkouts', total_checkouts.text)
    # Sets total renewals
    total_renewals = item_xml.find(ITEM_VAL_XPATH.format('TOT RENEW'))
    if total_renewals is not None:
        add_activity(item_key, 'renewals', total_renewals.text)
    return item_key


def main():
    pass

if __name__ == '__main__':
    main()
