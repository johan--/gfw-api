# Global Forest Watch API
# Copyright (C) 2013 World Resource Institute
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""This module supports accessing indicators data."""

import json

from gfw import cdb


class IndicatorSql(object):

    INDEX = """
        SELECT *
        FROM indicators
        LIMIT 20
    """

    SHOW = """
        SELECT *
        FROM indicators
        WHERE id = {id}
        LIMIT 1
    """


def _handler(response):
    if response.status_code == 200:
        data = json.loads(response.content)
        if 'rows' in data:
            return data['rows']
        else:
            return data
    else:
        raise Exception(response.content)


def _index(args):
    if not 'order' in args:
        args['order'] = ''
    if not 'interval' in args:
        args['interval'] = '12 Months'
    query = CountrySql.INDEX.format(**args)
    rows = _handler(cdb.execute(query))
    return dict(countries=rows)

def _show(args):
    query = CountrySql.SHOW.format(**args)
    rows = _handler(cdb.execute(query))
    return rows[0]


def execute(args):
    result = dict(params=args)

    if args.get('index'):
        result.update(_index(args))

    else:
        result.update(_show(args))

    return 'respond', result
