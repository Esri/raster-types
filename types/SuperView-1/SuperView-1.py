# ------------------------------------------------------------------------------
# Copyright 2018 Esri
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
# ------------------------------------------------------------------------------
# Name: SuperView-1.py
# Description: Create python raster type for SuperView-1
# Version: {20180924}
# Requirements: python.exe v2.7, standard python libraries
# Required Arguments:  N/A
# Optional Arguments: N/A
# Usage: Used through ArcMap/ArcPro as python raster type.
# Author: Esri Imagery Workflows Team
# ------------------------------------------------------------------------------
# !/usr/bin/env python

import os
import arcpy
import glob
import csv
import math

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class DataSourceType():
    Unknown = 0
    File = 1
    Folder = 2


class RasterTypeFactory():

    def getRasterTypesInfo(self):

        self.acquisitionDate_auxField = arcpy.Field()
        self.acquisitionDate_auxField.name = 'AcquisitionDate'
        self.acquisitionDate_auxField.aliasName = 'Acquisition Date'
        self.acquisitionDate_auxField.type = 'Date'
        self.acquisitionDate_auxField.length = 50

        self.cloudCover_auxField = arcpy.Field()
        self.cloudCover_auxField.name = 'CloudCover'
        self.cloudCover_auxField.aliasName = 'Cloud Cover'
        self.cloudCover_auxField.type = 'Double'
        self.cloudCover_auxField.precision = 5

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

        self.sensorAzimuth_auxField = arcpy.Field()
        self.sensorAzimuth_auxField.name = 'SensorAzimuth'
        self.sensorAzimuth_auxField.aliasName = 'Sensor Azimuth'
        self.sensorAzimuth_auxField.type = 'Double'
        self.sensorAzimuth_auxField.precision = 5

        self.sensorElevation_auxField = arcpy.Field()
        self.sensorElevation_auxField.name = 'SensorElevation'
        self.sensorElevation_auxField.aliasName = 'Sensor Elevation'
        self.sensorElevation_auxField.type = 'Double'
        self.sensorElevation_auxField.precision = 5

        self.sensorName_auxField = arcpy.Field()
        self.sensorName_auxField.name = 'SensorName'
        self.sensorName_auxField.aliasName = 'Sensor Name'
        self.sensorName_auxField.type = 'String'
        self.sensorName_auxField.length = 50

        return [{'rasterTypeName': 'SuperView-1',
                 'builderName': 'SuperView1Builder',
                 'description': ("Supports reading of SuperView-1 "
                                   "Level 1B/2A product metadata files"),
                 'supportsOrthorectification': True,
                 'enableClipToFootprint': True,
                 'isRasterProduct': True,
                 'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                 'dataSourceFilter': 'SV*.xml;SW*.dim',
                 'crawlerName': 'SuperView1Crawler',
                 'supportedUriFilters': [{'name': 'LEVEL1B',
                                          'allowedProducts': ['L1B'],
                                          'supportsOrthorectification': True,
                                          'supportedTemplates': ['Panchromatic',
                                                                 'Pansharpen',
                                                                 'Multispectral',
                                                                 'All Bands']},
                                         {'name': 'LEVEL2A',
                                          'allowedProducts': ['L2A'],
                                          'supportsOrthorectification': True,
                                          'supportedTemplates': ['Panchromatic',
                                                                 'Pansharpen',
                                                                 'Multispectral',
                                                                 'All Bands']},
                                         {'name': 'LEVEL3A',
                                          'allowedProducts': ['L3A'],
                                          'supportsOrthorectification': False,
                                          'supportedTemplates': ['Panchromatic',
                                                                 'Pansharpen',
                                                                 'Multispectral',
                                                                 'All Bands']}],
                 'productDefinitionName': 'SuperView1',
                 'processingTemplates': [{'name': 'Multispectral',
                                          'enabled': True,
                                          'outputDatasetTag': 'MS',
                                          'primaryInputDatasetTag': 'MS',
                                          'isProductTemplate': True,
                                          'functionTemplate': 'SV1_stretch_ms.rft.xml'},
                                         {'name': 'Panchromatic',
                                          'enabled': False,
                                          'outputDatasetTag': 'Pan',
                                          'primaryInputDatasetTag': 'Pan',
                                          'isProductTemplate': True,
                                          'functionTemplate': 'SV1_stretch_pan.rft.xml'},
                                         {'name': 'Pansharpen',
                                          'enabled': False,
                                          'outputDatasetTag': 'Pansharpened',
                                          'primaryInputDatasetTag': 'MS',
                                          'isProductTemplate': True,
                                          'functionTemplate': 'SV1_stretch_psh.rft.xml'},
                                         {'name': 'All Bands',
                                          'enabled': False,
                                          'isProductTemplate': False,
                                          'functionTemplate': 'SV1_stretch_allbands.rft.xml'}],
                 'bandProperties': [{'bandName': 'Blue',
                                     'bandIndex': 0,
                                     'wavelengthMin': 450.0,
                                     'wavelengthMax': 520.0,
                                     'datasetTag': 'MS'},
                                    {'bandName': 'Green',
                                     'bandIndex': 1,
                                     'wavelengthMin': 520.0,
                                     'wavelengthMax': 590.0,
                                     'datasetTag': 'MS'},
                                    {'bandName': 'Red',
                                     'bandIndex': 2,
                                     'wavelengthMin': 630.0,
                                     'wavelengthMax': 690.0,
                                     'datasetTag': 'MS'},
                                    {'bandName': 'NearInfrared',
                                     'bandIndex': 3,
                                     'wavelengthMin': 770.0,
                                     'wavelengthMax': 890.0,
                                     'datasetTag': 'MS'},
                                    {'bandName': 'Panchromatic',
                                     'bandIndex': 0,
                                     'wavelengthMin': 450.0,
                                     'wavelengthMax': 890.0,
                                     'datasetTag': 'Pan'}],
                 'fields': [self.acquisitionDate_auxField,
                            self.cloudCover_auxField,
                            self.sunAzimuth_auxField,
                            self.sunElevation_auxField,
                            self.sensorAzimuth_auxField,
                            self.sensorElevation_auxField,
                            self.sensorName_auxField]}]


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Utility functions used by the Builder and Crawler classes
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class Utilities():

    # checks if input data is from SuperView
    def isSuperView1(self, path):
        isSV1 = False
        try:
            dataFile = open(path)
        except BaseException:
            raise #Exception("Data file could not be opened")
#            return False
        data = dataFile.read()
        if '<SatelliteID>SV1' in data:
            isSV1 = True
        else:
            try:
                tree = ET.parse(path)
            except ET.ParseError as e:
                raise
                #print("Exception while parsing {0}\n{1}".format(path, e))
#                return None

            mission = tree.find('Dataset_Sources/Scene_Source/MISSION')
            if mission is not None:
                if 'SUPERVIEW' in mission.text:
                    isSV1 = True

        dataFile.close()
        return isSV1

    def __getTagFromTree(self, tree):
        # metadata has one parent root with all relevant metadata under it, hence\
        # taking the root
        try:
            root = tree.getroot()
            nBands = root.find('Bands')
            if nBands is None:
                nBands = tree.find('Raster_Dimensions/NBANDS')

            if nBands is not None:
                numBands = int(nBands.text)

            if numBands == 1:
                return 'Pan'
            if numBands >= 3:
                return 'MS'
        except BaseException:
            return None
        return None

    def getTag(self, path):
        # get tag by parsing the data tree
        try:
            tree = ET.parse(path)
        except ET.ParseError as e:
            print("Exception while parsing {0}\n{1}".format(path, e))
            return None

        return self.__getTagFromTree(tree)

    def getProductName(self, tree):
         # returns product level- 1B/2A/3A
        root = tree.getroot()
        productName = root.find('ProductLevel')
        if productName is None:
            productName = tree.find('Production/PRODUCT_TYPE')

        if productName is not None:
            return productName.text

        return None

    def getProductNameFromFile(self, path):
        # Get product name (level)
        try:
            tree = ET.parse(path)
        except ET.ParseError as e:
            print("Exception while parsing {0}\n{1}".format(path, e))
            return None

        return self.getProductName(tree)


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# SuperView builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class SuperView1Builder():

    def __init__(self, **kwargs):
        self.SensorName = 'SuperView-1'
        self.utilities = Utilities()

    def canOpen(self, datasetPath):
        # Open the datasetPath and check if the metadata file contains\
        # string 'SuperView', if not, the data is not added
        return self.utilities.isSuperView1(datasetPath)

    def build(self, itemURI):

        # constant values for pixel type and stretch based on pixel depth
        maxStretch16 = 2048
        pixelType16 = 5
        maxStretch8 = 256
        pixelType8 = 3

        # Make sure that the itemURI dictionary contains items
        if len(itemURI) <= 0:
            return None

        try:
            # ItemURI dictionary passed from crawler containing
            # path, tag, display name, group name, product type
            path = None
            if 'path' in itemURI:
                path = itemURI['path']
            else:
                return None

            srsEPSG = 0
            srsWKT = ''

            if path.endswith('.xml'):
                tree = ET.parse(path)
                root = tree.getroot()

                bands = root.find('Bands')
                if bands is not None:
                    noBands = int(bands.text)

                width = root.find('WidthInPixels')
                if width is not None:
                    cols = int(width.text)

                height = root.find('HeightInPixels')
                if height is not None:
                    rows = int(height.text)

                scenePathNode = root.find('ScenePath')
                if scenePathNode is not None:
                    scenePath = int(scenePathNode.text)

                sceneRowNode = root.find('SceneRow')
                if sceneRowNode is not None:
                    sceneRow = int(sceneRowNode.text)

                id = root.find('SatelliteID')
                if id is not None:
                    satelliteID = id.text

                # Dataset path: same name as the xml file
                fileName = os.path.splitext(path)[0] + '.tiff'
                fullPath = os.path.join(os.path.dirname(path), fileName)

                wgsSrs = arcpy.SpatialReference(4326)
                # Dataset frame - footprint; this is a list of Vertex
                # coordinates
                vertex_array = arcpy.Array()
                tlY = root.find('TopLeftLatitude')
                tlX = root.find('TopLeftLongitude')
                trY = root.find('TopRightLatitude')
                trX = root.find('TopRightLongitude')
                brY = root.find('BottomRightLatitude')
                brX = root.find('BottomRightLongitude')
                blY = root.find('BottomLeftLatitude')
                blX = root.find('BottomLeftLongitude')

                if tlX is not None and tlY is not None and trX is not None and trY is\
                        not None and brX is not None and brY is not None and\
                        blX is not None and blY is not None:
                    vertex_array.add(
                        arcpy.Point(
                            float(
                                tlX.text), float(
                                tlY.text)))
                    vertex_array.add(
                        arcpy.Point(
                            float(
                                trX.text), float(
                                trY.text)))
                    vertex_array.add(
                        arcpy.Point(
                            float(
                                brX.text), float(
                                brY.text)))
                    vertex_array.add(
                        arcpy.Point(
                            float(
                                blX.text), float(
                                blY.text)))

                footprint_geometry = arcpy.Polygon(vertex_array, wgsSrs)

                productLevel = root.find('ProductLevel')
                if productLevel is not None:
                    productType = productLevel.text

                    # Metadata for L1B does not contain map projection and map extent,\
                # EPSG of WGS and vertex extents are considered for srs and
                # rasterInfo footprint
                    if productType == 'LEVEL1B':
                        try:
                            xMin = float(tlX.text)
                            yMin = float(brY.text)
                            xMax = float(brX.text)
                            yMax = float(tlY.text)
                            srsEPSG = 4326
                        except BaseException:
                            raise Exception(
                                "Footprint for rasterInfo could not be created")
#                            return None

                    else:
                        # Horizontal CS (can also be a arcpy.SpatialReference object,
                        # ESPG code, path to a PRJ file or a WKT string)

                        # UTM Zone is not mentioned in metadata, calculating North/South \
                        # on basis of latitude-longitude values
                        centerLatPath = root.find('CenterLatitude')
                        centerLngPath = root.find('CenterLongitude')
                        if centerLatPath is not None:
                            centerLat = float(centerLatPath.text)
                        if centerLngPath is not None:
                            centerLng = float(centerLngPath.text)

                        lngd = round(centerLng, 7)
                        latd = round(centerLat, 8)

                        phi = math.radians(latd)
                        if (phi < 0):
                            uZone = 'S'
                        else:
                            uZone = 'N'

                        utmz = 1 + math.floor((lngd + 180) / 6)
                        utmZone = str(int(utmz)) + uZone

                        # eg. WGS 1984 UTM 39N
                        prjStr = 'WGS 1984 UTM zone ' + utmZone
                        utmSpatialRef = arcpy.SpatialReference(prjStr)
                        srsEPSG = utmSpatialRef.PCSCode

                        # rasterInfo footprint for L2A
                        minX = root.find('TopLeftMapX')
                        minY = root.find('BottomRightMapY')
                        maxX = root.find('BottomRightMapX')
                        maxY = root.find('TopLeftMapY')
                        if minX is not None and minY is not None and maxX\
                                is not None and maxY is not None:
                            try:
                                xMin = float(minX.text)
                                yMin = float(minY.text)
                                xMax = float(maxX.text)
                                yMax = float(maxY.text)
                            except BaseException:
                                raise Exception(
                                    "Footprint for rasterInfo could not be created")
#                                return None

                # Read pixel depth for stretch value and pixel type
                pixelDepthNode = root.find('PixelBits')
                if pixelDepthNode is not None:
                    pixelDepth = int(pixelDepthNode.text)

                if pixelDepth == 16:
                    maxInput = maxStretch16
                    pixelType = pixelType16
                if pixelDepth == 8:
                    maxInput = maxStretch8
                    pixelType = pixelType8

                # Metadata Information
                bandProperties = list()

                # Band info(part of metadata) - gain, bias etc

                # band order for Multispectral
                bandOrder = ['Blue', 'Green', 'Red', 'NearInfrared']
                gainNode = root.find('Gain')
                offsetNode = root.find('Offset')
                if gainNode is not None and offsetNode is not None:
                    # reading gain and offset values as a list from metadata
                    gain = [float(band) for band in (gainNode.text).split(',')]
                    offset = [float(band)
                              for band in (offsetNode.text).split(',')]

                    for i in range(len(gain)):
                        bandProperty = {}

                        if len(gain) == 1:
                            bandProperty['bandName'] = 'Panchromatic'
                        else:
                            bandProperty['bandName'] = bandOrder[i]

                        bandProperty['RadianceGain'] = gain[i]

                        bandProperty['RadianceBias'] = offset[i]

                        bandProperties.append(bandProperty)

                # Other metadata information (Sun elevation, azimuth etc)
                metadata = {}

                acquisitionDate = None
                acquisitionTime = None

                # Get the acquisition date of the scene
                acquisitionDate = root.find('CenterTime')
                if (acquisitionDate is not None):
                    metadata['AcquisitionDate'] = acquisitionDate.text

                sunElevation = root.find('SolarZenith')
                if sunElevation is not None:
                    metadata['SunElevation'] = float(sunElevation.text)

                sunAzimuth = root.find('SolarAzimuth')
                if sunAzimuth is not None:
                    metadata['SunAzimuth'] = float(sunAzimuth.text)

                sensorElevation = root.find('SatelliteZenith')
                if sensorElevation is not None:
                    metadata['SensorElevation'] = float(sensorElevation.text)

                sensorAzimuth = root.find('SatelliteAzimuth')
                if sensorAzimuth is not None:
                    metadata['SensorAzimuth'] = float(sensorAzimuth.text)

                # calculating the Viewing angle/Off Nadir angle
                pitchSat = root.find('PitchSatelliteAngle')
                rollSat = root.find('RollSatelliteAngle')
                if pitchSat is not None and rollSat is not None:
                    pitchAng = float(pitchSat.text)
                    rollAng = float(rollSat.text)
                    viewAngle = math.sqrt((pitchAng**2 + rollAng**2))
                    metadata['OffNadir'] = viewAngle

                cloud = root.find('CloudPercent')
                if cloud is not None:
                    if cloud.text is not None:
                        cloudCover = float(cloud.text)
                    else:
                        cloudCover = None

                metadata['ScenePath'] = scenePath
                metadata['SceneRow'] = sceneRow
                metadata['CloudCover'] = cloudCover

            elif path.endswith('.dim'):
                tree = ET.parse(path)

                srsWKT = 0
                projectionNode = tree.find(
                    'Coordinate_Reference_System/PROJECTION')
                if projectionNode is None:
                    projectionNode = tree.find(
                        'Dataset_Sources/Source_Information/Coordinate_Reference_System/Projection_OGCWKT')

                if projectionNode is not None:
                    srsWKT = projectionNode.text

                # Dataset path
                fileName = None
                filePathNode = tree.find(
                    'Data_Access/Data_File/DATA_FILE_PATH')
                if filePathNode is not None:
                    fileName = filePathNode.attrib['href']

                if fileName is None:
                    raise Exception("path not found")
#                    return None

                fullPath = os.path.join(os.path.dirname(path), fileName)

                # Dataset frame - footprint; this is a list of Vertex
                # coordinates
                vertex_array = arcpy.Array()
                all_vertex = tree.find('Dataset_Frame')
                if all_vertex is not None:
                    for vertex in all_vertex:
                        x_vertex = vertex.find('FRAME_LON')
                        y_vertex = vertex.find('FRAME_LAT')
                        if x_vertex is not None and y_vertex is not None:
                            frame_x = float(x_vertex.text)
                            frame_y = float(y_vertex.text)
                            vertex_array.add(arcpy.Point(frame_x, frame_y))

                    xMin = float(all_vertex[3].find('FRAME_LON').text)
                    xMax = float(all_vertex[1].find('FRAME_LON').text)
                    yMin = float(all_vertex[0].find('FRAME_LAT').text)
                    yMax = float(all_vertex[2].find('FRAME_LAT').text)

                footprint_geometry = arcpy.Polygon(vertex_array, srsWKT)

                # Read pixel depth from dim file
                pixelDepthNode = tree.find('Raster_Encoding/NBITS')
                if pixelDepthNode is not None:
                    pixelDepth = int(pixelDepthNode.text)

                if pixelDepth == 16:
                    maxInput = maxStretch16
                    pixelType = pixelType16
                if pixelDepth == 8:
                    maxInput = maxStretch8
                    pixelType = pixelType8

                # Metadata Information
                bandProperties = list()

                # Band info(part of metadata) - gain, bias etc
                img_interpretation = tree.find('Image_Interpretation')
                img_display = tree.find('Image_Display')
                noOfChildren = len(img_display.getchildren())

                if img_interpretation is not None:
                    for band_info in img_interpretation:
                        bandProperty = {}

                        band_desc = band_info.find('BAND_DESCRIPTION')
                        if band_desc is not None:
                            if band_desc.text == 'RED':
                                bandProperty['bandName'] = 'RED'
                            elif band_desc.text == 'GREEN':
                                bandProperty['bandName'] = 'GREEN'
                            elif band_desc.text == 'BLUE':
                                bandProperty['bandName'] = 'BLUE'
                            elif band_desc.text == 'NIR':
                                bandProperty['bandName'] = 'NearInfrared'
                            else:
                                bandProperty['bandName'] = band_desc.text

                        band_num = 0
                        band_index = band_info.find('BAND_INDEX')
                        if band_index is not None:
                            band_num = int(band_index.text)

                        gain = band_info.find('PHYSICAL_GAIN')
                        if gain is not None:
                            bandProperty['RadianceGain'] = float(gain.text)

                        bias = band_info.find('PHYSICAL_BIAS')
                        if bias is not None:
                            bandProperty['RadianceBias'] = float(bias.text)

                        unit = band_info.find('PHYSICAL_UNIT')
                        if unit is not None:
                            bandProperty['unit'] = unit.text

                        if img_display is not None:
                            for i in range(2, noOfChildren):
                                child = img_display.getchildren()[i]
                                if int(
                                        child.getchildren()[0].text) == band_num:
                                    min_ = float(child.getchildren()[3].text)
                                    max_ = float(child.getchildren()[4].text)
                                    stdv = float(child.getchildren()[1].text)
                                    mean = float(child.getchildren()[2].text)
                                    bandProperty['statistics'] = {
                                        'minimum': min_, 'maximum': max_, 'mean': mean, 'standardDeviation': stdv}

                        bandProperties.append(bandProperty)

                dimension = tree.find('Raster_Dimensions')
                if dimension is not None:
                    nCols = dimension.find('NCOLS')
                if nCols is not None:
                    cols = int(nCols.text)
                nRows = dimension.find('NROWS')
                if nRows is not None:
                    rows = int(nRows.text)
                nBands = dimension.find('NBANDS')
                if nBands is not None:
                    noBands = int(nBands.text)

                # Other metadata information (Sun elevation, azimuth etc)
                metadata = {}

                acquisitionDate = None
                acquisitionTime = None

                scene_source = 'Dataset_Sources/Source_Information/Scene_Source'
                img_metadata = tree.find(scene_source)
                if img_metadata is not None:
                    # Get the Sun Elevation
                    sunElevation = img_metadata.find('SUN_ELEVATION')
                    if sunElevation is not None:
                        metadata['SunElevation'] = float(sunElevation.text)

                    # Get the acquisition date of the scene
                    acquisitionDate = img_metadata.find('STOP_TIME')
                    if acquisitionDate is not None:
                        metadata['AcquisitionDate'] = acquisitionDate.text

                    # retrieve the view angle; this is the angle off Nadir view
                    viewingAngle = img_metadata.find('VIEWING_ANGLE')
                    if viewingAngle is None:
                        viewingAngle = img_metadata.find('VIEWING_ANGLE')

                    if viewingAngle is not None:
                        metadata['OffNadir'] = float(viewingAngle.text)

                    instrument = img_metadata.find('INSTRUMENT')
                    if instrument is not None:
                        metadata['Instrument'] = instrument.text

                    # Get the Sun Azimuth
                    sunAzimuth = img_metadata.find('SUN_AZIMUTH')
                    if sunAzimuth is not None:
                        metadata['SunAzimuth'] = float(sunAzimuth.text)

                    # Get the Sun Distance
                    sunDistance = img_metadata.find('EARTH_SUN_DISTANCE')
                    if sunDistance is not None:
                        metadata['SunDistance'] = float(sunDistance.text)

                # Get the Cloud Cover
                qaChildren = tree.find(
                    'Dataset_Sources/Source_Information/Quality_Assessment').getchildren()
                for qaChild in qaChildren:
                    qpChildren = qaChild.getchildren()
                    if len(qpChildren) > 0:
                        if qpChildren[0].text == "CLOUD_COVER_PERCENTAGE":
                            if qpChildren[2].text is not None:
                                cloudCover = float(qpChildren[2].text)

                if cloudCover is not None:
                    metadata['CloudCover'] = cloudCover

         # adding RPC parameters from rpb file available in dataset
            try:
                coeffVal = []
                try:
                    rpcFileName = os.path.splitext(path)[0] + '.rpb'
                    #rpcPath = os.path.join(os.path.dirname(path), rpcFileName)
                    data = open(rpcFileName, 'r')
                except BaseException:
                    raise Exception("RPC file could not be opened")

                rpc = data.read()
                rpc = rpc.replace('\t', '').split('(')
                metadata['CONSTANTZ'] = (
                    rpc[0].split('heightOffset = ')[1]).split(";")[0]
                predata = rpc[0]
                predata = predata.split('IMAGE')[1].split('lineNumCoef')[0]
                predata = predata.split(';')
                predata = [float(val.split('=')[1]) for val in predata[0:-1]]

                lineNumCoef = [float(val)
                               for val in rpc[1].split(')')[0].split(',')]
                lineDenCoef = [float(val)
                               for val in rpc[2].split(')')[0].split(',')]
                sampNumCoef = [float(val)
                               for val in rpc[3].split(')')[0].split(',')]
                sampDenCoef = [float(val)
                               for val in rpc[4].split(')')[0].split(',')]

                # list of 90 RPC values
                coeffVal = predata[2:] + lineNumCoef + \
                    lineDenCoef + sampNumCoef + sampDenCoef
            except Exception:
                coeffVal = []
            RPC = {
                'GeodataTransforms': [
                    {
                        'geodataTransform': 'RPC',
                        'geodataTransformArguments': {
                            'coeff': coeffVal
                        }
                    }
                ]
            }
            try:
                geoDataTransform = str(RPC).replace("'", '"')
            except Exception:
                geoDataTransform = ''

            metadata['SensorName'] = self.SensorName
            metadata['bandProperties'] = bandProperties
            metadata['ProductType'] = self.utilities.getProductName(tree)

            # define a dictionary of variables
            variables = {}
            variables['DefaultMaximumInput'] = maxInput
            variables['DefaultGamma'] = 1

            # define a dictionary with information that helps to add raster product\
            # without opening the datafile
            rasterInfo = {}
            rasterInfo['pixelType'] = pixelType
            rasterInfo['nCols'] = cols
            rasterInfo['nRows'] = rows
            rasterInfo['nBands'] = noBands

            if srsEPSG:
                rasterInfo['spatialReference'] = srsEPSG
            else:
                rasterInfo['spatialReference'] = srsWKT

            rasterInfo['geodataXform'] = geoDataTransform
            rasterInfo['XMin'] = xMin
            rasterInfo['YMin'] = yMin
            rasterInfo['XMax'] = xMax
            rasterInfo['YMax'] = yMax

            # Assemble everything into an outgoing dictionary
            builtItem = {}
            builtItem['raster'] = {'uri': fullPath, 'rasterInfo': rasterInfo}
            builtItem['footprint'] = footprint_geometry
            builtItem['keyProperties'] = metadata
            builtItem['variables'] = variables
            builtItem['itemUri'] = itemURI

            if srsEPSG:
                builtItem['spatialReference'] = srsEPSG
            else:
                builtItem['spatialReference'] = srsWKT

            builtItem['noData'] = [0]

            builtItemsList = list()
            builtItemsList.append(builtItem)
            return builtItemsList

        except BaseException:
            raise


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# SuperView Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class SuperView1Crawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()
        self.paths = crawlerProperties['paths']
        self.recurse = crawlerProperties['recurse']
        self.filter = crawlerProperties['filter']
        if not self.filter:
            self.filter = 'SV*.xml;SW*.dim'
        try:
            self.pathGenerator = self.createGenerator()

        except StopIteration:
            return None

    def createGenerator(self):
        fileFilter = self.filter.split(';')
        fileFilter = list(filter(None, fileFilter))

        for path in self.paths:
            if not os.path.exists(path):
                continue

            # handles paths with different folder levels
            if os.path.isdir(path):
                if self.recurse:
                    for root, dirs, files in (os.walk(path)):
                        for file in (files):
                            if file.endswith(fileFilter[0][1:]) or file.endswith(
                                    fileFilter[1][1:]):
                                filename = os.path.join(root, file)
                                yield filename
                else:
                    for filterToScan in fileFilter:
                        filter_to_scan = path + os.path.sep + filterToScan
                        for filename in glob.glob(filter_to_scan):
                            yield filename

            elif path.endswith(".csv"):
                with open(path, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    rasterFieldIndex = -1
                    firstRow = next(reader)

                    # Check for the 'raster' field in the csv file, if not\
                    # present take the first field as input data
                    for attribute in firstRow:
                        if attribute.lower() == 'raster':
                            rasterFieldIndex = firstRow.index(attribute)
                            break

                    if rasterFieldIndex == -1:
                        csvfile.seek(0)
                        rasterFieldIndex = 0

                    for row in reader:
                        filename = row[rasterFieldIndex]
                        if (filename.endswith(fileFilter[0][1:]) or file.endswith(
                                fileFilter[1][1:])) and os.path.exists(filename):
                            yield filename

            elif path.endswith(fileFilter[0][1:]) or path.endswith(fileFilter[1][1:]):
                paths = [path]
                path2 = ''
                # enables Pansharpen option in raster products
                if 'MUX' in path:
                    path2 = path.replace('MUX', 'PAN')
                elif 'PAN' in path:
                    path2 = path.replace('PAN', 'MUX')

                # if corresponding MUX/PAN file exists, create the URI which\
                # will be used to enable Pansharpen
                if os.path.exists(path2):
                    paths.append(path2)

                for pathName in paths:
                    yield pathName

    def __iter__(self):
        return self

    def next(self):
        # Return URI dictionary to Builder
        return self.getNextUri()

    def getNextUri(self):

        try:
            self.curPath = next(self.pathGenerator)
            curTag = self.utils.getTag(self.curPath)
            productName = self.utils.getProductNameFromFile(self.curPath)

            # If the tag or productName was not found in the metadata file or\
            # there was some exception raised, we move on to the next item
            if curTag is None or productName is None:
                return self.getNextUri()

        except StopIteration:
            return None

        uri = {
            'path': self.curPath,
            'displayName': os.path.splitext(os.path.basename(self.curPath))[0],
            'tag': curTag,
            'groupName': os.path.split(os.path.dirname(self.curPath))[1],
            'productName': productName
        }

        return uri
