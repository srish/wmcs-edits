#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Tabulate total edit actions and edit actions originating from Cloud VPS
# instances in a date range per-wiki.
#
# Copyright (c) 2019 Wikimedia Foundation and contributors
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
# more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import datetime
import ipaddress
import itertools
import logging
import dns.resolver
import pymysql

WMCS_NETWORKS = [
    ipaddress.IPv4Network(net) for net in [
        # eqiad
        '10.68.0.0/24',
        '10.68.16.0/21',
        '172.16.0.0/21',
        '10.68.32.0/24',
        '10.68.48.0/24',
        # codfw
        '10.196.0.0/24',
        '10.196.16.0/21',
        '172.16.128.0/21',
        '10.196.32.0/24',
        '10.196.48.0/24',
    ]
]

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_date(p_dt):
    """ Checks if the date is in the following format '%Y-%m-%d'
    """
    try:
        return datetime.datetime.strptime(p_dt, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError('Not a valid date: "%s"' % p_dt)


def strcspn(string, pred):
    return len(list(itertools.takewhile(lambda x: x not in pred, string)))


def pairwise(iterable):
    a = iter(iterable)
    return zip(a, a)


def conf_file(name):
    return open('/srv/mediawiki-config/{}'.format(name)).read()


def dblist(name):
    dbs = []
    for line in conf_file("dblists/{}.dblist".format(name)).splitlines():
        line = line[0:strcspn(line, '#')].strip()
        if line[0:2] == '%%':
            dbs = eval_dblist(line)
        else:
            dbs.append(line)
    return set(dbs)


def eval_dblist(line):
    terms = line.strip('% ').split()
    result = dblist(terms.pop())
    for op, part in pairwise(terms):
        if op == '+':
            result = result + dblist(part)
        elif op == '-':
            result = result - dblist(part)
    return list(result)


def get_public_open_wikis():
    return dblist('all') - dblist('closed') - dblist('private')


def get_slice(dbname):
    for s in ['s1', 's2', 's4', 's5', 's6', 's7', 's8']:
        if dbname in dblist(s):
            return s
    return 's3'


def get_conn(dbname):
    s = get_slice(dbname)
    ans = dns.resolver.query(
        '_{}-analytics._tcp.eqiad.wmnet'.format(s),
        'SRV'
    )[0]
    return pymysql.connect(
        host=str(ans.target),
        port=ans.port,
        db=dbname,
        read_default_file='/etc/mysql/conf.d/analytics-research-client.cnf',
        charset='utf8mb4',
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_edit_counts(dbname, startts, endts):
    """Count WMCS edits and total edits for given date range in given db."""
    wmcs_edits = 0
    total_edits = 0
    connection = get_conn(dbname)
    with connection.cursor() as cur:
        cur.execute("""
        SELECT cuc_ip FROM cu_changes
        WHERE cuc_timestamp > %s AND cuc_timestamp < %s
        """, (startts, endts))
        for row in cur:
            total_edits += 1
            try:
                ip = ipaddress.IPv4Address(row['cuc_ip'].decode('utf-8'))
            except ipaddress.AddressValueError:
                # not an IPv4 address, skip it
                continue
            for network in WMCS_NETWORKS:
                if ip in network:
                    wmcs_edits += 1
                    continue
    return {
        'wmcs': wmcs_edits,
        'total': total_edits
    }


def calc_wmcs_edits(starttime, endttime):
    """Calculate the number of all / WMCS edits for all open wikis in a given
    time period.

    Return a dict of 'dbname' => { 'all': <all-edits-count>, 'wmcs': <wmcs-edit-count>}"""
    stats = {}
    for dbname in get_public_open_wikis():
        logging.info('Processing %s', dbname)
        try:
            stats[dbname] = get_edit_counts(dbname, starttime, endttime)
        except pymysql.MySQLError as e:
            logging.exception('Skipping %s: %s', dbname, e)
    return stats


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Daily visit count')
    PARSER.add_argument('-s', '--start', metavar='YYYY-MM-DD', required=True, \
        type=parse_date, help='Start date (inclusive)')
    PARSER.add_argument('-e', '--end', metavar='YYYY-MM-DD', type=parse_date, \
        help='End date (exclusive)')
    ARGS = PARSER.parse_args()

    if not ARGS.end:
        ARGS.end = ARGS.start + datetime.timedelta(1)

    DAYS = 90
    NOW = datetime.datetime.utcnow()
    START = (NOW - datetime.timedelta(DAYS)).strftime('%Y%m%d000000')
    CUTOFF = NOW.strftime('%Y%m%d000001')

    DATA = calc_wmcs_edits(
        ARGS.start.strftime('%Y%m%d000000'),
        ARGS.end.strftime('%Y%m%d000000'),
    )

    GRAND_TOTAL = 0
    WMCS_TOTAL = 0
    for wiki in sorted(DATA.keys()):
        t = DATA[wiki]['total']
        w = DATA[wiki]['wmcs']
        GRAND_TOTAL += t
        WMCS_TOTAL += w
        print('{},{},{}'.format(wiki, t, w))
    print('{},{},{}'.format('TOTAL', GRAND_TOTAL, WMCS_TOTAL))
