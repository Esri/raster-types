#------------------------------------------------------------------------------
# Copyright 2022 Esri
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------

import os
import arcpy
import glob
import csv
import json
import copy
import requests as _requests



class DataSourceType():
    Unknown = 0
    File = 1
    Folder = 2
    String = 4


class RasterTypeFactory():

    def getRasterTypesInfo(self):

        self.acquisitionDate_auxField = arcpy.Field()
        self.acquisitionDate_auxField.name = 'AcquisitionDate'
        self.acquisitionDate_auxField.aliasName = 'Acquisition Date'
        self.acquisitionDate_auxField.type = 'Date'
        self.acquisitionDate_auxField.length = 50

        self.instrument_auxField = arcpy.Field()
        self.instrument_auxField.name = 'Instrument'
        self.instrument_auxField.aliasName = 'Instrument'
        self.instrument_auxField.type = 'String'
        self.instrument_auxField.length = 50

        self.sensorName_auxField = arcpy.Field()
        self.sensorName_auxField.name = 'SensorName'
        self.sensorName_auxField.aliasName = 'Sensor Name'
        self.sensorName_auxField.type = 'String'
        self.sensorName_auxField.length = 50

        self.sunAzimuth_auxField = arcpy.Field()
        self.sunAzimuth_auxField.name = 'SunAzimuth'
        self.sunAzimuth_auxField.aliasName = 'Sun Azimuth'
        self.sunAzimuth_auxField.type = 'Double'
        self.sunAzimuth_auxField.precision = 5

        self.sunElevation_auxField = arcpy.Field()
        self.sunElevation_auxField.name = 'SunElevation'
        self.sunElevation_auxField.aliasName = 'Sun Elevation'
        self.sunElevation_auxField.type = 'Double'
        self.sunElevation_auxField.precision = 5

        self.cloudCover_auxField = arcpy.Field()
        self.cloudCover_auxField.name = 'CloudCover'
        self.cloudCover_auxField.aliasName = 'Cloud Cover'
        self.cloudCover_auxField.type = 'Double'
        self.cloudCover_auxField.precision = 5

        return [
                {
                    'rasterTypeName': 'STAC',
                    'builderName': 'STACBuilder',
                    'description': ("STAC Raster Type"),
                    'isRasterProduct': False,
                    'dataSourceType': (DataSourceType.File),
                    'crawlerName': 'STACCrawler',
                    'dataSourceFilter': '*.json',
                    'fields': [self.sensorName_auxField,
            self.acquisitionDate_auxField,
            self.instrument_auxField,
            self.sunAzimuth_auxField,
            self.sunElevation_auxField,
            self.cloudCover_auxField]
                }
               ]


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Utility functions used by the Builder and Crawler classes
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class Utilities():

    def isSTAC(self, path):

        return True

    def get_stac_file(self, item, type):

        if type in item["assets"]:
            href = item["assets"][type]["href"]
            return href
        else:
            links = item["links"]
            for i in range(len(links)):
                if links[i]["rel"] ==  type:
                    href = links[i]["href"]
                    return href
            return None
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# STAC builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class STACBuilder():

    def __init__(self, **kwargs):
        self.utilities = Utilities()

    def canOpen(self, datasetPath):
        return self.utilities.isSTAC(datasetPath)

    def build(self, itemURI):

        # Make sure that the itemURI dictionary contains items
        if len(itemURI) <= 0:
            return None

        try:

            # ItemURI dictionary passed from crawler containing
            # path, tag, display name, group name, product type
            path = None
            if 'path' in itemURI:
                path = json.loads(itemURI['path'])
            else:
                return None

            #footprint_geometry = arcpy.Polygon(vertex_array)

            # Other metadata information (Sun elevation, azimuth etc)
            metadata = {}


            acquisitionDate = None

            # Get the Sun Elevation
            img_metadata = path["attribute_dict"]

            OffNadir = img_metadata.get('OffNadir')
            if OffNadir is not None:
                metadata['OffNadir'] = float(OffNadir)

            sunElevation = img_metadata.get('SunElevation')
            if sunElevation is not None:
                metadata['SunElevation'] = float(sunElevation)

            # Get the acquisition date of the scene
            acquisitionDate = img_metadata.get('AcquisitionDate')
            if acquisitionDate is not None:
                metadata['AcquisitionDate'] = acquisitionDate

            # Get the Instrument
            instrument = img_metadata.get('Instrument')
            if instrument is not None:
                metadata['Instrument'] = str(instrument)

            # Get the Sun Azimuth
            sunAzimuth = img_metadata.get('SunAzimuth')
            if sunAzimuth is not None:
                metadata['SunAzimuth'] = float(sunAzimuth)

            # Get the CloudCover
            cloudCover = img_metadata.get('CloudCover')
            if cloudCover is not None:
                metadata['CloudCover'] = float(cloudCover)

            # Get the SensorName
            SensorName = img_metadata.get('SensorName')
            if SensorName is not None:
                metadata['SensorName'] = SensorName

            B1 = self.utilities.get_stac_file(path, "B1")
            B2 = self.utilities.get_stac_file(path, "B2")
            B3 = self.utilities.get_stac_file(path, "B3")
            B4 = self.utilities.get_stac_file(path, "B4")
            B5 = self.utilities.get_stac_file(path, "B5")
            B6 = self.utilities.get_stac_file(path, "B6")
            B7 = self.utilities.get_stac_file(path, "B7")
            B8 = self.utilities.get_stac_file(path, "B8")
            B9 = self.utilities.get_stac_file(path, "B9")
            B10 = self.utilities.get_stac_file(path, "B10")
            B11 = self.utilities.get_stac_file(path, "B11")

            builtItem = {}
            builtItem['raster'] = {
                    'functionDataset': {
                        'rasterFunction': "AllBands.rft.xml",
                        'rasterFunctionArguments': {
                            'Raster1' : B1,
                            'Raster2' : B2,
                            'Raster3' : B3,
                            'Raster4' : B4,
                            'Raster5' : B5,
                            'Raster6' : B6,
                            'Raster7' : B7,
                            'Raster8' : B8,
                            'Raster9' : B9,
                            'Raster10' : B10,
                            'Raster11' : B11}
                        }
                    }

            builtItem['keyProperties'] = metadata
            #builtItem['itemUri'] = itemURI

            builtItemsList = list()
            builtItemsList.append(builtItem)
            return builtItemsList

        except:
            raise


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# STAC Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class STACCrawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()

        f = open(crawlerProperties["paths"][0])
        crawlerProperties = json.load(f)
        self.api = crawlerProperties.get('stac_api')
        self.query =  crawlerProperties.get('query')
        self.attribute_dict = crawlerProperties.get('attribute_dict')

        try:
            self.pathGenerator = self.createGenerator()

        except StopIteration:
            return None

    def createGenerator(self):
        if not isinstance(self.api, str):
            raise RuntimeError(f"Invalid STAC API URL-\n{self.api}")
        api_search_endpoint = (
            self.api + "search" if self.api.endswith("/") else self.api + "/search"
        )

        request_params = None

        if request_params is None:
            request_params = {}
        if not isinstance(request_params, dict):
            raise RuntimeError("request_params should be of type dictionary")
        if any(key in request_params for key in ["url", "params", "data", "json"]):
            raise RuntimeError(
                "request_params cannot contain these keys : url, params, data or json"
            )

        request_method = "POST"
        if not isinstance(request_method, str) or request_method.upper() not in [
            "GET",
            "POST",
        ]:
            raise RuntimeError(
                "request_method can only be one of the following: GET or POST"
            )

        new_query = None
        if self.query is not None:
            if not isinstance(self.query, dict):
                raise RuntimeError("parameter query should be of type dictionary")
            new_query = copy.deepcopy(self.query)

        if request_method.upper() == "GET":
            data = _requests.get(
                api_search_endpoint, params=new_query, **request_params
            )
        else:
            data = _requests.post(api_search_endpoint, json=new_query, **request_params)

        if data.status_code != 200 or data.headers.get("content-type") not in [
            "application/json",
            "application/geo+json",
            "application/json;charset=utf-8",
        ]:
            raise RuntimeError(
                f"Invalid Response: Please verify that the STAC API URL and the specified query are correct-\n{data.text}"
            )

        json_data = data.json()
        if "type" not in json_data or json_data["type"] != "FeatureCollection":
            raise RuntimeError(
                f"Invalid JSON Response from the STAC API: Please verify that the STAC API URL and the specified query are correct-\n{json_data}"
            )
        items = json_data["features"]

        rc_attribute_dict = {}
        self.attribute_dict = {} if self.attribute_dict is None else self.attribute_dict
        for i, item in enumerate(items):
            for key in self.attribute_dict:
                if self.attribute_dict[key] in item:
                    rc_attribute_dict[key]=item[self.attribute_dict[key]]
                elif self.attribute_dict[key] in item["properties"]:
                    rc_attribute_dict[key]= item["properties"][self.attribute_dict[key]]
                else:
                    rc_attribute_dict[key] = key
            items[i].update({"attribute_dict":rc_attribute_dict})
        for item in items:
            yield item

    def __iter__(self):
        return self

    def next(self):
        ## Return URI dictionary to Builder
        return self.getNextUri()
       

    def getNextUri(self):
        
        try:
            self.curPath = next(self.pathGenerator)
            
        except StopIteration:
            return None

        uri = {
                'path': json.dumps(self.curPath)
              }

        return uri




