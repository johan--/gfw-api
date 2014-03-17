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
import datetime

from google.appengine.api import urlfetch

# def cloudMask(img):
#     # 61440 = Cloudy (high probability) Cirrus
#     # 53248 = Cloudy (high probability) Non-Cirrus
#     # 28672 = Cloudy (low probability) Cirrus
#     quality = img.select('BQA')
#     cloud_hc = quality.eq(61440)
#     cloud_hn = quality.eq(53248)
#     cloud_lc = quality.eq(28672)
#     masked_image = img.mask()
#     # .and(cloud_hc.or(cloud_hn).or(cloud_lc).not())
#     logging.info(dir(masked_image))
#     return img.mask(masked_image)


# VizParams = 

# var image2 = collection.median().clip(polygon);
# collectionMasked = collection.map(maskL8);
# var image3 = collectionMasked.median().clip(polygon);


def createBox(lon, lat, w, h, ccw = True):
    """Returns the coordinates of the corners of the box around the
    supplied latitude and longitude of the centroid, with the width
    and height of the box equal to `h` and `w` in meters.

    """
    h_deg = (h / 2) / (60.* 1602.) 
    w_deg = (w / 2) / (60.* 1602.) 
    coords= [[lon + w_deg, lat + h_deg],
             [lon - w_deg, lat + h_deg],
             [lon - w_deg, lat - h_deg],
             [lon + w_deg, lat - h_deg],
             [lon + w_deg, lat + h_deg]]
    return coords


def hsvpan(rgb, gray):
    """Pan-sharpen Landsat 8."""
    huesat = rgb.rgbtohsv().select(['hue', 'saturation'])
    upres = ee.Image.cat(huesat, gray).hsvtorgb()
    return upres


def landsatID(alert_date, coords, offset_days=30):
    """Returns the ID of the Landsat 8 image that is closest to the
    supplied alert date within the supplied GEE-formatted polygon

    """

    d = datetime.datetime.strptime(alert_date, '%Y-%m-%d')
    begin_date = d - datetime.timedelta(days=offset_days)
    poly = ee.Feature.Polygon(coords)
    coll = ee.ImageCollection('LANDSAT/LC8_L1T_TOA').filterDate(begin_date, alert_date)
    coll = coll.filterBounds(poly)

    # descending sort by acquisition time
    desc = coll.sort('system:time_start', False).limit(1)
    return desc.getInfo()['features'][0]['id']


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
    
    coords = createBox(lon, lat, w, h)
    image_id = landsatID(date, coords)
    loc = 'LANDSAT/%s' % image_id

    input = ee.Image(loc)
    rgb = input.select("B4","B3","B2")
    pan = input.select("B8")
    sharp = hsvpan(rgb, pan)

    vis_params = {'min':0.01, 'max':0.5, 'gamma':1.2}
    visual_image = sharp.visualize(**vis_params)
    params = {'scale':30, 'crs':'EPSG:4326', 'region':str(coords)}
    url = visual_image.getThumbUrl(params)
    return url

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
