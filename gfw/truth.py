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

"""This module supports the Truth API."""

import config
import copy
import ee
import json
import logging

from google.appengine.api import urlfetch


def _boom_hammer(lat, lon, h, w, date, res, asset, fmt):
    """Return URL to Earth Engine results for supplied params dictionary.

    Args:
        lat - decimal latitude
        lon - decimal longitude
        h - desired image pixel height
        w - desired image pixel width
        date - YYYY-MM-DD
        res - desired resolution (thumb | true)
        asset - Earth Engine asset
        fmt - desired output format (img | raw)"""
    # TODO(hammer)
    return 'http://goo.gl/BqwvZG'


def _params_prep(params):
    """Return prepared params ready to go as dict."""
    lat, lon = map(float, params.get('ll').split(','))
    h, w = map(int, params.get('dim').split(','))
    res = 'true' if 'res' not in params else params.get('res')
    fmt = 'img' if 'fmt' not in params else params.get('fmt')
    date = params.get('date')
    asset = params.get('asset')
    keys = ['lat', 'lon', 'h', 'w', 'res', 'fmt', 'date', 'asset']
    return dict(zip(keys, [lat, lon, h, w, res, fmt, date, asset]))


def _fetch_url(url):
    """Return raw response content from supplied url."""
    rpc = urlfetch.create_rpc(deadline=50)
    urlfetch.make_fetch_call(rpc, url)
    return rpc.get_result()


def find(params):
    """Find and return truth from supplied params."""
    boom = _params_prep(params)
    logging.info(boom)
    ee.Initialize(config.EE_CREDENTIALS, config.EE_URL)
    ee.data.setDeadline(60000)
    url = _boom_hammer(**boom)
    result = _fetch_url(url)
    return result
