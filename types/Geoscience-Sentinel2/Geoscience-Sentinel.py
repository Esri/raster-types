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
# Name: Geoscience-Sentinel
# Description: Geoscience-Sentinel python raster type.
# Version: 20190925
# Requirements: python.exe v2.7 and above, standard python libraries, python yaml library(https://pyyaml.org/wiki/PyYAMLDocumentation), ArcMap , python requests module , python boto3 module
# Required Arguments:  N/A
# Optional Arguments: N/A
# Usage: Used through ArcMap/ArcPro as python raster type.
# Author: Esri Imagery Workflows Team
# ------------------------------------------------------------------------------

import os
import arcpy
import glob
import csv
import requests

try:
    import yaml
    import boto3
except ImportError as e:
    raise

class DataSourceType():
    File = 1
    Folder = 2

class RasterTypeFactory():

    def getRasterTypesInfo(self):
        self.datastrip_id_auxField = arcpy.Field()
        self.datastrip_id_auxField.name = 'DatastripId'
        self.datastrip_id_auxField.aliasName = 'Datastrip_Id'
        self.datastrip_id_auxField.type = 'string'
        self.datastrip_id_auxField.length = 200

        self.clcoverp_auxField = arcpy.Field()
        self.clcoverp_auxField.name = 'CloudCoverPercentage'
        self.clcoverp_auxField.aliasName = 'Cloud Cover Percentage'
        self.clcoverp_auxField.type = 'double'
        self.clcoverp_auxField.precision = 5

##        self.datatake_identifier_auxField = arcpy.Field()
##        self.datatake_identifier_auxField.name = 'DataTakeIdentifier'
##        self.datatake_identifier_auxField.aliasName = 'Data Take Identifier'
##        self.datatake_identifier_auxField.type = 'string'
##        self.datatake_identifier_auxField.precision = 200

        self.dacq_auxField = arcpy.Field()
        self.dacq_auxField.name = 'AcquisitionDate'
        self.dacq_auxField.aliasName = 'Acquisition Date'
        self.dacq_auxField.type = 'date'

##        self.ancdp_auxField = arcpy.Field()
##        self.ancdp_auxField.name = 'DegradedANC_DataPercentage'
##        self.ancdp_auxField.aliasName = 'Degraded anc Data Percentage'
##        self.ancdp_auxField.type = 'double'
##        self.ancdp_auxField.length = 5

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

##        self.msidp_auxField = arcpy.Field()
##        self.msidp_auxField.name = 'DegradedMSI_DataPercentage'
##        self.msidp_auxField.aliasName = 'Degraded MSI Data Percentage'
##        self.msidp_auxField.type = 'double'
##        self.msidp_auxField.precision = 5

        self.reflconv_auxField = arcpy.Field()
        self.reflconv_auxField.name = 'ReflectanceConversion'
        self.reflconv_auxField.aliasName = 'Reflectance Conversion'
        self.reflconv_auxField.type = 'double'
        self.reflconv_auxField.precision = 50

##        self.saturated_auxField = arcpy.Field()
##        self.saturated_auxField.name = 'Saturated'
##        self.saturated_auxField.aliasName = 'Saturated'
##        self.saturated_auxField.type = 'double'
##        self.saturated_auxField.precision = 10

        self.tile_ref_auxField = arcpy.Field()
        self.tile_ref_auxField.name = 'TileReference'
        self.tile_ref_auxField.aliasName = 'Tile Reference'
        self.tile_ref_auxField.type = 'String'
        self.tile_ref_auxField.length = 50

##        self.Orbit_auxField = arcpy.Field()
##        self.Orbit_auxField.name = 'Orbit'
##        self.Orbit_auxField.aliasName = 'Orbit'
##        self.Orbit_auxField.type = 'float'
##        self.Orbit_auxField.precision = 10

##        self.Orbit_direction_auxField = arcpy.Field()
##        self.Orbit_direction_auxField.name = 'OrbitDirection'
##        self.Orbit_direction_auxField.aliasName = 'Orbit Direction'
##        self.Orbit_direction_auxField.type = 'string'
##        self.Orbit_direction_auxField.precision = 200

        self.platform_code_auxField = arcpy.Field()
        self.platform_code_auxField.name = 'PlatformCode'
        self.platform_code_auxField.aliasName = 'Platform Code'
        self.platform_code_auxField.type = 'String'
        self.platform_code_auxField.length = 200

##        self.proc_baseline_auxField = arcpy.Field()
##        self.proc_baseline_auxField.name = 'ProcessingBaseline'
##        self.proc_baseline_auxField.aliasName = 'Processing Baseline'
##        self.proc_baseline_auxField.type = 'double'
##        self.proc_baseline_auxField.length = 10

        self.processingLevel_auxField = arcpy.Field()
        self.processingLevel_auxField.name = 'ProcessingLevel'
        self.processingLevel_auxField.aliasName = 'Processing Level'
        self.processingLevel_auxField.type = 'String'
        self.processingLevel_auxField.length = 200

        self.prod_format_auxField = arcpy.Field()
        self.prod_format_auxField.name = 'ProductFormat'
        self.prod_format_auxField.aliasName = 'Product Format'
        self.prod_format_auxField.type = 'String'
        self.prod_format_auxField.length = 200


##        self.prod_uri_auxField = arcpy.Field()
##        self.prod_uri_auxField.name = 'ProductURI'
##        self.prod_uri_auxField.aliasName = 'Product URI'
##        self.prod_uri_auxField.type = 'String'
##        self.prod_uri_auxField.length = 200

        self.tile_id_auxField = arcpy.Field()
        self.tile_id_auxField.name = 'TileID'
        self.tile_id_auxField.aliasName = 'Tile ID'
        self.tile_id_auxField.type = 'String'
        self.tile_id_auxField.length = 200

        return [
                {
                    'rasterTypeName': 'Geoscience-Sentinel',
                    'builderName': 'GeoscienceSentinelBuilder',
                    'description': ("Supports reading of Geoscience Sentinel2 Level1 and Level2"),
                    'supportsOrthorectification': True,
                    'enableClipToFootprint': True,
                    'isRasterProduct': False,
                    'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                    'dataSourceFilter': 'L2*METADATA.yaml',
                    'crawlerName': 'GeoscienceSentinelCrawler',
                    'productDefinitionName': 'Geoscience-Sentinel',
                    'supportedUriFilters': [
                                            {
                                                'name': 'Level2',
                                                'allowedProducts': [
                                                                    'S2MSIARD',
                                                                    'ard'
                                                                   ],
                                                'supportsOrthorectification': True,
                                                'enableClipToFootprint': True,
                                                'supportedTemplates': [
                                                                       'Geoscience_MS_ALL',
                                                                       'Geoscience_MS_Supplementary',
                                                                       'Geoscience_MS_QA',
                                                                       'Geoscience_MS_Lambertian',
                                                                       'Geoscience_MS_NBAR',
                                                                       'Geoscience_MS_NBART'
                                                                      ]
                                            },

                                            {
                                                'name': 'Level1',
                                                'allowedProducts': [
                                                                    'S2MSIARD',
                                                                    'ard'
                                                                   ],
                                                'supportsOrthorectification': True,
                                                'supportedTemplates': [
                                                                       'Geoscience_MS_Lambertian'
                                                                      ]
                                            }
                                           ],
                    'processingTemplates': [
                                            {
                                                'name': 'Geoscience_MS_ALL',
                                                'enabled': True,
                                                'outputDatasetTag': 'MS',
                                                'primaryInputDatasetTag': 'MS',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_ALL.rft.xml'
                                            },
                                            {
                                                'name': 'Geoscience_MS_Supplementary',
                                                'enabled': False,
                                                'outputDatasetTag': 'Supplementary',
                                                'primaryInputDatasetTag': 'Supplementary',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_Supplementary.rft.xml'
                                            },

                                            {
                                                'name': 'Geoscience_MS_QA',
                                                'enabled': False,
                                                'outputDatasetTag': 'QA',
                                                'primaryInputDatasetTag': 'QA',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_QA.rft.xml'
                                            },

                                            {
                                                'name': 'Geoscience_MS_Lambertian',
                                                'enabled': False,
                                                'outputDatasetTag': 'Lambertian',
                                                'primaryInputDatasetTag': 'Lambertian',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_Lambertian.rft.xml'
                                            },

                                            {
                                                'name': 'Geoscience_MS_NBAR',
                                                'enabled': False,
                                                'outputDatasetTag': 'NBAR',
                                                'primaryInputDatasetTag': 'NBAR',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_NBAR.rft.xml'
                                            },

                                            {
                                                'name': 'Geoscience_MS_NBART',
                                                'enabled': False,
                                                'outputDatasetTag': 'NBART',
                                                'primaryInputDatasetTag': 'NBART',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_NBART.rft.xml'
                                            }
                                           ],
                    #GET THE CORRECT BAND INDEX , MIN MAX WAVELENGTH
                    'bandProperties': [
                                        {
                                            'bandName': 'azimuthal_exiting',
                                            'bandIndex': 1,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'azimuthal_incident',
                                            'bandIndex': 2,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'exiting',
                                            'bandIndex': 3,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'fmask',
                                            'bandIndex': 4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'incident',
                                            'bandIndex': 5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_blue',
                                            'bandIndex': 6,
                                            'wavelengthMin': 447.6,
                                            'wavelengthMax': 545.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_coastal_aerosol',
                                            'bandIndex': 7,
                                            'wavelengthMin': 430.4,
                                            'wavelengthMax': 457.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_contiguity',
                                            'bandIndex': 8,
                                            'wavelengthMin': 1550.0,
                                            'wavelengthMax': 1590.0,
                                            'datasetTag': 'CG'
                                        },
                                        {
                                            'bandName': 'lambertian_green',
                                            'bandIndex': 9,
                                            'wavelengthMin': 537.5,
                                            'wavelengthMax': 582.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_nir_1',
                                            'bandIndex': 10,
                                            'wavelengthMin': 762.6,
                                            'wavelengthMax': 907.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_nir_2',
                                            'bandIndex': 11,
                                            'wavelengthMin': 848.3,
                                            'wavelengthMax': 881.3,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_red',
                                            'bandIndex': 12,
                                            'wavelengthMin': 645.5,
                                            'wavelengthMax': 683.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_red_edge_1',
                                            'bandIndex': 13,
                                            'wavelengthMin': 694.4,
                                            'wavelengthMax': 713.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_red_edge_2',
                                            'bandIndex': 14,
                                            'wavelengthMin': 731.2,
                                            'wavelengthMax': 749.2,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_red_edge_3',
                                            'bandIndex': 15,
                                            'wavelengthMin': 768.5,
                                            'wavelengthMax': 796.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'lambertian_swir_2',
                                            'bandIndex': 16,
                                            'wavelengthMin': 1542.2,
                                            'wavelengthMax': 1685.2,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'lambertian_swir_3',
                                            'bandIndex': 17,
                                            'wavelengthMin': 2081.4,
                                            'wavelengthMax': 2323.4,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'nbar_blue',
                                            'bandIndex': 18,
                                            'wavelengthMin': 447.6,
                                            'wavelengthMax': 545.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_coastal_aerosol',
                                            'bandIndex': 19,
                                            'wavelengthMin': 430.4,
                                            'wavelengthMax': 457.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_contiguity',
                                            'bandIndex': 20,
                                            'datasetTag': 'CG'
                                        },
                                        {
                                            'bandName': 'nbar_green',
                                            'bandIndex': 21,
                                            'wavelengthMin': 537.5,
                                            'wavelengthMax': 582.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_nir_1',
                                            'bandIndex': 22,
                                            'wavelengthMin': 762.6,
                                            'wavelengthMax': 907.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_nir_2',
                                            'bandIndex': 23,
                                            'wavelengthMin': 848.3,
                                            'wavelengthMax': 881.3,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_red',
                                            'bandIndex': 24,
                                            'wavelengthMin': 645.5,
                                            'wavelengthMax': 683.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_red_edge_1',
                                            'bandIndex': 25,
                                            'wavelengthMin': 694.4,
                                            'wavelengthMax': 713.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_red_edge_2',
                                            'bandIndex': 26,
                                            'wavelengthMin': 731.2,
                                            'wavelengthMax': 749.2,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_red_edge_3',
                                            'bandIndex': 27,
                                            'wavelengthMin': 768.5,
                                            'wavelengthMax': 796.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbar_swir_2',
                                            'bandIndex': 28,
                                            'wavelengthMin': 1542.2,
                                            'wavelengthMax': 1685.2,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'nbar_swir_3',
                                            'bandIndex': 29,
                                            'wavelengthMin': 2081.4,
                                            'wavelengthMax': 2323.4,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'nbart_blue',
                                            'bandIndex': 30,
                                            'wavelengthMin': 447.6,
                                            'wavelengthMax': 545.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_coastal_aerosol',
                                            'bandIndex': 31,
                                            'wavelengthMin': 430.4,
                                            'wavelengthMax': 457.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_contiguity',
                                            'bandIndex': 32,
                                            'datasetTag': 'CG'
                                        },
                                        {
                                            'bandName': 'nbart_green',
                                            'bandIndex': 33,
                                            'wavelengthMin': 537.5,
                                            'wavelengthMax': 582.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_nir_1',
                                            'bandIndex': 34,
                                            'wavelengthMin': 762.6,
                                            'wavelengthMax': 907.6,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_nir_2',
                                            'bandIndex': 35,
                                            'wavelengthMin': 848.3,
                                            'wavelengthMax': 881.3,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_red',
                                            'bandIndex': 36,
                                            'wavelengthMin': 645.5,
                                            'wavelengthMax': 683.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_red_edge_1',
                                            'bandIndex': 37,
                                            'wavelengthMin': 694.4,
                                            'wavelengthMax': 713.4,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_red_edge_2',
                                            'bandIndex': 38,
                                            'wavelengthMin': 731.2,
                                            'wavelengthMax': 749.2,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_red_edge_3',
                                            'bandIndex': 39,
                                            'wavelengthMin': 768.5,
                                            'wavelengthMax': 796.5,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'nbart_swir_2',
                                            'bandIndex': 40,
                                            'wavelengthMin': 1542.2,
                                            'wavelengthMax': 1685.2,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'nbart_swir_3',
                                            'bandIndex': 41,
                                            'wavelengthMin': 2081.4,
                                            'wavelengthMax': 2323.4,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'relative_azimuth',
                                            'bandIndex': 42,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'relative_slope',
                                            'bandIndex': 43,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'satellite_azimuth',
                                            'bandIndex': 44,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'satellite_view',
                                            'bandIndex': 45,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'solar_azimuth',
                                            'bandIndex': 46,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'solar_zenith',
                                            'bandIndex': 47,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'terrain_shadow',
                                            'bandIndex': 48,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'timedelta',
                                            'bandIndex': 49,
                                            'datasetTag': 'MS'
                                        }
                                      ],
                    #GET THE CORRECT BAND INDEX , MIN MAX WAVELENGTH

                    'fields': [self.datastrip_id_auxField,
                               self.clcoverp_auxField,
##                               self.datatake_identifier_auxField,
                               self.dacq_auxField,
##                               self.ancdp_auxField,
                               self.sunAzimuth_auxField,
                               self.sunElevation_auxField,
##                               self.msidp_auxField,
                               self.reflconv_auxField,
##                               self.saturated_auxField,
                               self.tile_ref_auxField,
##                               self.Orbit_auxField,
##                               self.Orbit_direction_auxField,
                               self.platform_code_auxField,
##                               self.proc_baseline_auxField,
                               self.processingLevel_auxField,
                               self.prod_format_auxField,
##                               self.prod_uri_auxField,
                               self.tile_id_auxField]
                }
               ]

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Utility functions used by the Builder and Crawler classes
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class Utilities():

    def readYaml(self,path):        #to read the yaml file located locally.
        try:
            with open(path, 'r') as q:
                try:
                    doc = (yaml.load(q))
                except yaml.YAMLError as exc:
                    raise
                return doc
        except:
            raise
    def readYamlS3(self,path):          #to read the yaml file located on S3
        page=requests.get(path,stream=True,timeout=None)
        try:
            doc= (yaml.load(page.content))
        except yaml.YAMLError as exc:
            raise
        return doc

    def readYamlS3_boto3(self,bucket,path):          #to read the yaml file located on S3
        client = boto3.client('s3')
        try:
            page = client.get_object(Bucket=bucket,Key=path)
            doc = (yaml.load(page['Body'].read()))
        except yaml.YAMLError as exc:
            raise
        return doc

    def getProductName(self, doc):
        try:
            productName = doc['product_type']
            if (productName is not None):
                return productName
        except:
            return None
        return None

    def getProcessingLevel(self, doc):
        try:
            processingLevel = doc['processing_level']
            if (processingLevel is not None):
                return processingLevel
        except:
            return None
        return None



# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Geoscience builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class GeoscienceSentinelBuilder():

    def __init__(self, **kwargs):
        self.SensorName = 'Geoscience'
        self.utils = Utilities()

    def canOpen(self, datasetPath):
        return True

    def embedMRF(self,inputDir,fileName,cols,rows,dtype,nodata,maxX,maxY,minX,minY,prjString,protocol):

        try:
            #create template for 10m resolution and write to a file
            cachingMRF = \
                '<MRF_META>\n'  \
                '  <CachedSource>\n'  \
                '    <Source>/{12}{0}/{1}</Source>\n'  \
                '  </CachedSource>\n'  \
                '  <Raster>\n'  \
                '    <Size c="1" x="{2}" y="{3}"/>\n'  \
                '    <PageSize c="1" x="512" y="512"/>\n'  \
                '    <Compression>LERC</Compression>\n'  \
                '    <DataType>{4}</DataType>\n'  \
                '  <DataValues NoData="{6}"/>\n' \
                '  <DataFile>z:/mrfcache/Geoscience/Sentinel2/{0}/{1}/{5}.mrf_cache</DataFile><IndexFile>z:/mrfcache/Geoscience/Sentinel2/{0}/{1}/{5}.mrf_cache</IndexFile></Raster>\n'  \
                '  <Rsets model="uniform" scale="2"/>\n'  \
                '  <GeoTags>\n'  \
                '    <BoundingBox maxx="{7}" maxy="{8}" minx="{9}" miny="{10}"/>\n'  \
                '    <Projection>{11}</Projection>\n'  \
                '  </GeoTags>\n'  \
                '  <Options>V2=ON</Options>\n'  \
                '</MRF_META>\n'.format(inputDir,fileName,cols,rows,dtype,fileName[0:-4],nodata,maxX,maxY,minX,minY,prjString,protocol)

        except Exception as exp:
            log.Message(str(exp),log.const_critical_text)

        return cachingMRF


    def build(self, itemURI):
     # Make sure that the itemURI dictionary contains items
        if (len(itemURI) <= 0):
            return None
        try:
            # ItemURI dictionary passed from crawler containing
            # path, tag, display name, group name, product type
            path=None
            if ('path' in itemURI):
                _yamlpath = itemURI['path']
            else:
                return None

             #for each band in the image generate the path
            L01=L02=L03=L04=L05=L06=L07=L08=L09=L10=L11=L12=NR01=NR02=NR03=NR04=NR05=NR06=NR07=NR08=NR09=NR10=NR11=NR12=NRT01=NRT02=NRT03=NRT04=NRT05=NRT06=NRT07=NRT08=NRT09=NRT10=NRT11=NRT12=IN=FM=TD=EX=AI=AE=RA=RS=SA=SV=SZ=SZE=TS=""
            yamldir = os.path.dirname(_yamlpath)

            if (_yamlpath.startswith("http:")):
                doc = self.utils.readYamlS3(_yamlpath)

                if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
##                    print  ('Err. Invalid input format!')
                    return False

                lastIdx= _yamlpath.rfind('/')
##                startIdx= _yamlpath.find('.com')+5  #plus 5 to get the index of the character after .com/
                inputDir= _yamlpath[7:lastIdx]  #along with the bucket name
                protocol ='vsicurl/http://'

                refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                maxX= refPoints['lr']['x']
                maxY= refPoints['ur']['y']
                minX= refPoints['ll']['x']
                minY= refPoints['ll']['y']

                prjString= doc['grid_spatial']['projection']['spatial_reference']

                AE = self.embedMRF(inputDir,(doc['image']['bands']['azimuthal_exiting']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                AI = self.embedMRF(inputDir,(doc['image']['bands']['azimuthal_incident']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                EX = self.embedMRF(inputDir,(doc['image']['bands']['exiting']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                FM = self.embedMRF(inputDir,(doc['image']['bands']['fmask']['path']),"5490","5490","","0",maxX,maxY,minX,minY,prjString,protocol)
                IN = self.embedMRF(inputDir,(doc['image']['bands']['incident']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

                L01 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L02 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L03 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                L04 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L05 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L06 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L07 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L08 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L09 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L10 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L11 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L12 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                NR01 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR02 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR03 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                NR04 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR05 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR06 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR07 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR08 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR09 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR10 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR11 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR12 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT03 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                NRT04 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT05 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT06 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT09 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT10 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT11 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT12 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                RA = self.embedMRF(inputDir,(doc['image']['bands']['relative_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                RS = self.embedMRF(inputDir,(doc['image']['bands']['relative_slope']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SA = self.embedMRF(inputDir,(doc['image']['bands']['satellite_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SV = self.embedMRF(inputDir,(doc['image']['bands']['satellite_view']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

                SZ = self.embedMRF(inputDir,(doc['image']['bands']['solar_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SZE = self.embedMRF(inputDir,(doc['image']['bands']['solar_zenith']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                TS = self.embedMRF(inputDir,(doc['image']['bands']['terrain_shadow']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                TD = self.embedMRF(inputDir,(doc['image']['bands']['timedelta']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

            elif (_yamlpath.startswith("s3:")):
                index = _yamlpath.find("/",5) #giving a start index of 5 will ensure that the / from s3:// is not returned.
                bucketname = _yamlpath[5:index] #First 5 letters will always be s3://
                key = _yamlpath[index+1:]
                doc = self.utils.readYamlS3_boto3(bucketname,key)

                if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
##                    print  ('Err. Invalid input format!')
                    return False

                lastIdx= _yamlpath.rfind('/')
                inputDir= _yamlpath[5:lastIdx]  #along with the bucket name
                protocol ='vsis3/'

                refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                maxX= refPoints['lr']['x']
                maxY= refPoints['ur']['y']
                minX= refPoints['ll']['x']
                minY= refPoints['ll']['y']

                prjString= doc['grid_spatial']['projection']['spatial_reference']

                AE = self.embedMRF(inputDir,(doc['image']['bands']['azimuthal_exiting']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                AI = self.embedMRF(inputDir,(doc['image']['bands']['azimuthal_incident']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                EX = self.embedMRF(inputDir,(doc['image']['bands']['exiting']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                FM = self.embedMRF(inputDir,(doc['image']['bands']['fmask']['path']),"5490","5490","","0",maxX,maxY,minX,minY,prjString,protocol)
                IN = self.embedMRF(inputDir,(doc['image']['bands']['incident']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

                L01 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L02 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L03 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                L04 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L05 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L06 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L07 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L08 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L09 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L10 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L11 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                L12 = self.embedMRF(inputDir,(doc['image']['bands']['lambertian_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                NR01 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR02 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR03 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                NR04 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR05 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR06 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR07 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR08 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR09 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR10 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR11 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NR12 = self.embedMRF(inputDir,(doc['image']['bands']['nbar_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_blue']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_coastal_aerosol']['path']),"1830","1830","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT03 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_contiguity']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                NRT04 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_green']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT05 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_nir_1']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT06 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_nir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red']['path']),"10980","10980","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_1']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT09 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT10 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_red_edge_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT11 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_swir_2']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)
                NRT12 = self.embedMRF(inputDir,(doc['image']['bands']['nbart_swir_3']['path']),"5490","5490","Int16","-999",maxX,maxY,minX,minY,prjString,protocol)

                RA = self.embedMRF(inputDir,(doc['image']['bands']['relative_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                RS = self.embedMRF(inputDir,(doc['image']['bands']['relative_slope']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SA = self.embedMRF(inputDir,(doc['image']['bands']['satellite_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SV = self.embedMRF(inputDir,(doc['image']['bands']['satellite_view']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

                SZ = self.embedMRF(inputDir,(doc['image']['bands']['solar_azimuth']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                SZE = self.embedMRF(inputDir,(doc['image']['bands']['solar_zenith']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)
                TS = self.embedMRF(inputDir,(doc['image']['bands']['terrain_shadow']['path']),"5490","5490","","",maxX,maxY,minX,minY,prjString,protocol)
                TD = self.embedMRF(inputDir,(doc['image']['bands']['timedelta']['path']),"5490","5490","Float32","-999",maxX,maxY,minX,minY,prjString,protocol)

            else:
                doc = self.utils.readYaml(_yamlpath)

                if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
##                    print  ('Err. Invalid input format!')
                    return False

                refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                maxX= refPoints['lr']['x']
                maxY= refPoints['ur']['y']
                minX= refPoints['ll']['x']
                minY= refPoints['ll']['y']

                AE = os.path.join(yamldir,(doc['image']['bands']['azimuthal_exiting']['path']))
                AI = os.path.join(yamldir,(doc['image']['bands']['azimuthal_incident']['path']))
                EX = os.path.join(yamldir,(doc['image']['bands']['exiting']['path']))
                FM = os.path.join(yamldir,(doc['image']['bands']['fmask']['path']))
                IN = os.path.join(yamldir,(doc['image']['bands']['incident']['path']))

                L01 = os.path.join(yamldir,(doc['image']['bands']['lambertian_blue']['path']))
                L02 = os.path.join(yamldir,(doc['image']['bands']['lambertian_coastal_aerosol']['path']))
                L03 = os.path.join(yamldir,(doc['image']['bands']['lambertian_contiguity']['path']))
                L04 = os.path.join(yamldir,(doc['image']['bands']['lambertian_green']['path']))
                L05 = os.path.join(yamldir,(doc['image']['bands']['lambertian_nir_1']['path']))
                L06 = os.path.join(yamldir,(doc['image']['bands']['lambertian_nir_2']['path']))
                L07 = os.path.join(yamldir,(doc['image']['bands']['lambertian_red']['path']))
                L08 = os.path.join(yamldir,(doc['image']['bands']['lambertian_red_edge_1']['path']))
                L09 = os.path.join(yamldir,(doc['image']['bands']['lambertian_red_edge_2']['path']))
                L10 = os.path.join(yamldir,(doc['image']['bands']['lambertian_red_edge_3']['path']))
                L11 = os.path.join(yamldir,(doc['image']['bands']['lambertian_swir_2']['path']))
                L12 = os.path.join(yamldir,(doc['image']['bands']['lambertian_swir_3']['path']))

                NR01 = os.path.join(yamldir,(doc['image']['bands']['nbar_blue']['path']))
                NR02 = os.path.join(yamldir,(doc['image']['bands']['nbar_coastal_aerosol']['path']))
                NR03 = os.path.join(yamldir,(doc['image']['bands']['nbar_contiguity']['path']))
                NR04 = os.path.join(yamldir,(doc['image']['bands']['nbar_green']['path']))
                NR05 = os.path.join(yamldir,(doc['image']['bands']['nbar_nir_1']['path']))
                NR06 = os.path.join(yamldir,(doc['image']['bands']['nbar_nir_2']['path']))
                NR07 = os.path.join(yamldir,(doc['image']['bands']['nbar_red']['path']))
                NR08 = os.path.join(yamldir,(doc['image']['bands']['nbar_red_edge_1']['path']))
                NR09 = os.path.join(yamldir,(doc['image']['bands']['nbar_red_edge_2']['path']))
                NR10 = os.path.join(yamldir,(doc['image']['bands']['nbar_red_edge_3']['path']))
                NR11 = os.path.join(yamldir,(doc['image']['bands']['nbar_swir_2']['path']))
                NR12 = os.path.join(yamldir,(doc['image']['bands']['nbar_swir_3']['path']))

                NRT01 = os.path.join(yamldir,(doc['image']['bands']['nbart_blue']['path']))
                NRT02 = os.path.join(yamldir,(doc['image']['bands']['nbart_coastal_aerosol']['path']))
                NRT03 = os.path.join(yamldir,(doc['image']['bands']['nbart_contiguity']['path']))
                NRT04 = os.path.join(yamldir,(doc['image']['bands']['nbart_green']['path']))
                NRT05 = os.path.join(yamldir,(doc['image']['bands']['nbart_nir_1']['path']))
                NRT06 = os.path.join(yamldir,(doc['image']['bands']['nbart_nir_2']['path']))
                NRT07 = os.path.join(yamldir,(doc['image']['bands']['nbart_red']['path']))
                NRT08 = os.path.join(yamldir,(doc['image']['bands']['nbart_red_edge_1']['path']))
                NRT09 = os.path.join(yamldir,(doc['image']['bands']['nbart_red_edge_2']['path']))
                NRT10 = os.path.join(yamldir,(doc['image']['bands']['nbart_red_edge_3']['path']))
                NRT11 = os.path.join(yamldir,(doc['image']['bands']['nbart_swir_2']['path']))
                NRT12 = os.path.join(yamldir,(doc['image']['bands']['nbart_swir_3']['path']))

                RA = os.path.join(yamldir,(doc['image']['bands']['relative_azimuth']['path']))
                RS = os.path.join(yamldir,(doc['image']['bands']['relative_slope']['path']))
                SA = os.path.join(yamldir,(doc['image']['bands']['satellite_azimuth']['path']))
                SV = os.path.join(yamldir,(doc['image']['bands']['satellite_view']['path']))

                SZ = os.path.join(yamldir,(doc['image']['bands']['solar_azimuth']['path']))
                SZE = os.path.join(yamldir,(doc['image']['bands']['solar_zenith']['path']))
                TS = os.path.join(yamldir,(doc['image']['bands']['terrain_shadow']['path']))
                TD = os.path.join(yamldir,(doc['image']['bands']['timedelta']['path']))


            #Metadata Information
            #bandProperties = [{'bandName':'azimuthal_exiting'},{'bandName':'azimuthal_incident'},{'bandName':'exiting'},{'bandName':'fmask'},{'bandName':'incident'},{'bandName':'lambertian_blue'},{'bandName':'lambertian_contiguity'},{'bandName':'lambertian_green'},{'bandName':'lambertian_nir'},{'bandName':'lambertian_red'},{'bandName':'lambertian_swir_1'},{'bandName':'lambertian_swir_2'},{'bandName':'nbar_blue'},{'bandName':'nbar_contiguity'},{'bandName':'nbar_green'},{'bandName':'nbar_nir'},{'bandName':'nbar_red'},{'bandName':'nbar_swir_1'},{'bandName':'nbar_swir_2'},{'bandName':'nbart_blue'},{'bandName':'nbart_contiguity'},{'bandName':'nbart_green'},{'bandName':'nbart_nir'},{'bandName':'nbart_red'},{'bandName':'nbart_swir_1'},{'bandName':'nbart_swir_2'},{'bandName':'relative_azimuth'},{'bandName':'relative_slope'},{'bandName':'satellite_azimuth'},{'bandName':'satellite_view'},{'bandName':'sbt_contiguity'},{'bandName':'sbt_thermal_infrared'},{'bandName':'solar_azimuth'},{'bandName':'solar_zenith'},{'bandName':'terrain_shadow'},{'bandName':'timedelta'}]
            metadata = {}
            try:
                source=doc['lineage']['source_datasets']['level1']
            except:
                source=doc['lineage']['source_datasets']['S2MSI1C']


            #source = readyaml(_yamlpath)
            if (source is not None):
                # Get the Sun Elevation
                SunElevation = source['image']['sun_elevation']
                if (SunElevation is not None):
                    metadata['SunElevation']=float(SunElevation)

                # Get the Sun Azimuth
                sunAzimuth = source['image']['sun_azimuth']
                if (sunAzimuth is not None):
                    metadata['SunAzimuth']=float(sunAzimuth)

                # Get the Cloud Cover Percentage
                clcoverp = source['image']['cloud_cover_percentage']
                if (clcoverp is not None):
                    metadata['CloudCoverPercentage']=float(clcoverp)

                #Get the Data Take Identifier
##                datatake_identifier = source['datatake_id']['datatakeIdentifier']
##                if (datatake_identifier is not None):
##                    metadata['DataTakeIdentifier']= (datatake_identifier)

                #Get the Degraded ANC Data Percentage
##                ancdp = source['image']['degraded_anc_data_percentage']
##                if (ancdp is not None):
##                    metadata['DegradedANC_DataPercentage']=float(ancdp)

                # Get the Degraded MSI Data Percentage
##                msidp = source['image']['degraded_msi_data_percentage']
##                if (msidp is not None):
##                    metadata['DegradedMSI_DataPercentage']=float(msidp)

                # Get the Date Acquisition
                dacq = source['datatake_sensing_start']
                if (dacq is not None):
                    metadata['AcquisitionDate']=(dacq)[0:19] #milliseconds part is not supported when added as a string

                #Get the Reflectance Conversion
                reflconv = source['image']['reflectance_conversion']
                if (reflconv is not None):
                    metadata['ReflectanceConversion']=float(reflconv)

                #Get the Saturated
##                saturated = source['image']['saturated']
##                if (saturated is not None):
##                    metadata['Saturated']=float(saturated)

                #Get the Tile Reference
                tile_ref = source['image']['tile_reference']
                if (tile_ref is not None):
                    metadata['TileReference']=str(tile_ref)

                #Get the DataStrip ID
                datastripid = source['datastrip_id']
                if (tile_ref is not None):
                    metadata['DatastripId']=str(datastripid)

                #Get the Orbit
##                Orbit = source['orbit']
##                if (Orbit is not None):
##                    metadata['Orbit']=float(Orbit)

                #Get the Orbit Direction
##                Orbit_direction = source['orbit_direction']
##                if (Orbit_direction is not None):
##                    metadata['OrbitDirection']=str(Orbit_direction)

                #Get the Plaform Code
                platform_code = doc['platform']['code'] #doc['lineage']['source_datasets']['LS_USGS_L1C1']['processing_level']
                if (platform_code is not None):
                    metadata['PlatformCode']=platform_code

                #Get the Processing Baseline
##                proc_baseline = source['processing_baseline']
##                if (proc_baseline is not None):
##                    metadata['ProcessingBaseline']=float(proc_baseline)

                #Get the Processing Level
                processingLevel = doc['processing_level']
                if (processingLevel is not None):
                    metadata['ProcessingLevel']=str(processingLevel)

                #Get the Product Format
                prod_format = source['product_format']['name']
                if (prod_format is not None):
                    metadata['ProductFormat']=str(prod_format)

                #Get the Platform Code
                platform_code = source['platform']['code']
                if (platform_code is not None):
                    metadata['PlatformCode']=str(platform_code)

                #Get the Product Name
                productName = doc['product_type']
                if (productName is not None):
                    metadata['ProductName']=str(productName)

                #Get the Product URI
##                prod_uri = source['product_uri']
##                if (prod_uri is not None):
##                    metadata['ProductURI']=str(prod_uri)

                #Get the Tile ID
                tile_id = source['tile_id']
                if (tile_id is not None):
                    metadata['TileID']=str(tile_id)


################# ESPG CODE
            srsWKT = 0
            projectionNode = doc['grid_spatial']['projection']['spatial_reference']
            if (projectionNode is not None):
                srsWKT = projectionNode

##                sr = arcpy.SpatialReference()
##                sr.loadFromString(srsWKT)

##            vertex_array = arcpy.Array()
##            coords= doc['grid_spatial']['projection']['valid_data']['coordinates'][0]
##            if coords:
##                for point in coords:
##                    vertex_array.add(arcpy.Point(point[0],point[1]))
##
##                footprint_geometry = arcpy.Polygon(vertex_array,sr)

################# DEFINE A DICTIONARY OF VARIABLES
            variables = {}

            builtItem = {}
            #Depending upon the tag name in the itemURI pass the appropriate bandProperties dictionary and RFT
            if (itemURI['tag'] == "MS"):
                bandProperties = [{'bandName':'azimuthal_exiting'},
                                                    {'bandName':'azimuthal_incident'},
                                                    {'bandName':'exiting'},
                                                    {'bandName':'fmask'},
                                                    {'bandName':'incident'},
                                                    {'bandName':'lambertian_blue'},
                                                    {'bandName':'lambertian_coastal_aerosol'},
                                                    {'bandName':'lambertian_contiguity'},
                                                    {'bandName':'lambertian_green'},
                                                    {'bandName':'lambertian_red'},
                                                    {'bandName':'lambertian_red_edge_1'},
                                                    {'bandName':'lambertian_red_edge_2:'},
                                                    {'bandName':'lambertian_red_edge_3'},
                                                    {'bandName':'lambertian_nir_1'},
                                                    {'bandName':'lambertian_nir_2'},
                                                    {'bandName':'lambertian_swir_2'},
                                                    {'bandName':'lambertian_swir_3'},
                                                    {'bandName':'nbar_blue'},
                                                    {'bandName':'nbar_coastal_aerosol'},
                                                    {'bandName':'nbar_contiguity'},
                                                    {'bandName':'nbar_green'},
                                                    {'bandName':'nbar_red'},
                                                    {'bandName':'nbar_red_edge_1'},
                                                    {'bandName':'nbar_red_edge_2'},
                                                    {'bandName':'nbar_red_edge_3'},
                                                    {'bandName':'nbar_nir_1'},
                                                    {'bandName':'nbar_nir_2'},
                                                    {'bandName':'nbar_swir_2'},
                                                    {'bandName':'nbar_swir_3'},
                                                    {'bandName':'nbart_blue'},
                                                    {'bandName':'nbart_coastal_aerosol'},
                                                    {'bandName':'nbart_contiguity'},
                                                    {'bandName':'nbart_green'},
                                                    {'bandName':'nbart_red'},
                                                    {'bandName':'nbart_red_edge_1'},
                                                    {'bandName':'nbart_red_edge_2'},
                                                    {'bandName':'nbart_red_edge_3'},
                                                    {'bandName':'nbart_nir_1'},
                                                    {'bandName':'nbart_nir_2'},
                                                    {'bandName':'nbart_swir_2'},
                                                    {'bandName':'nbart_swir_3'},
                                                    {'bandName':'relative_azimuth'},
                                                    {'bandName':'relative_slope'},
                                                    {'bandName':'satellite_azimuth'},
                                                    {'bandName':'satellite_view'},
                                                    {'bandName':'solar_azimuth'},
                                                    {'bandName':'solar_zenith'},
                                                    {'bandName':'terrain_shadow'},
                                                    {'bandName':'timedelta'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"Geoscience_ALL_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': AE,
                                                                                    'Raster1_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': AI,
                                                                                    'Raster2_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': EX,
                                                                                    'Raster3_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': FM,
                                                                                    'Raster4_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': IN,
                                                                                    'Raster5_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': L02,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': L01,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': L03,
                                                                                    'Raster8_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': L04,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': L07,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster11': L08,
                                                                                    'Raster11_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster12': L09,
                                                                                    'Raster12_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster13': L10,
                                                                                    'Raster13_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster14': L05,
                                                                                    'Raster14_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster15': L06,
                                                                                    'Raster15_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster16': L11,
                                                                                    'Raster16_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster17': L12,
                                                                                    'Raster17_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster18': NR02,
                                                                                    'Raster18_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster19': NR01,
                                                                                    'Raster19_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster20': NR03,
                                                                                    'Raster20_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster21': NR04,
                                                                                    'Raster21_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster22': NR07,
                                                                                    'Raster22_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster23': NR08,
                                                                                    'Raster23_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster24': NR09,
                                                                                    'Raster24_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster25': NR10,
                                                                                    'Raster25_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster26': NR05,
                                                                                    'Raster26_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster27': NR06,
                                                                                    'Raster27_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster28': NR11,
                                                                                    'Raster28_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster29': NR12,
                                                                                    'Raster29_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster30': NRT02,
                                                                                    'Raster30_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster31': NRT01,
                                                                                    'Raster31_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster32': NRT03,
                                                                                    'Raster32_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster33': NRT04,
                                                                                    'Raster33_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster34': NRT07,
                                                                                    'Raster34_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster35': NRT08,
                                                                                    'Raster35_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster36': NRT09,
                                                                                    'Raster36_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster37': NRT10,
                                                                                    'Raster37_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster38': NRT05,
                                                                                    'Raster38_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster39': NRT06,
                                                                                    'Raster39_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster40': NRT11,
                                                                                    'Raster40_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster41': NRT12,
                                                                                    'Raster41_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster42': RA,
                                                                                    'Raster42_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster43': RS,
                                                                                    'Raster43_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster44': SA,
                                                                                    'Raster44_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster45': SV,
                                                                                    'Raster45_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster46': SZ,
                                                                                    'Raster46_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster47': SZE,
                                                                                    'Raster47_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster48': TS,
                                                                                    'Raster48_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster49': TD,
                                                                                    'Raster49_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                    }
                                                        }
                                    }

            elif (itemURI['tag'] == "Supplementary"):
                #Supplementary
                bandProperties = [{'bandName':'azimuthal_exiting'},
                                    {'bandName':'azimuthal_incident'},
                                    {'bandName':'exiting'},
                                    {'bandName':'incident'},
                                    {'bandName':'relative_azimuth'},
                                    {'bandName':'relative_slope'},
                                    {'bandName':'satellite_azimuth'},
                                    {'bandName':'satellite_view'},
                                    {'bandName':'solar_azimuth'},
                                    {'bandName':'solar_zenith'},
                                    {'bandName':'timedelta'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"GS_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': AE,
                                                                                    'Raster1_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': AI,
                                                                                    'Raster2_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': EX,
                                                                                    'Raster3_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': IN,
                                                                                    'Raster4_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': RA,
                                                                                    'Raster5_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': RS,
                                                                                    'Raster6_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': SA,
                                                                                    'Raster7_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': SV,
                                                                                    'Raster8_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': SZ,
                                                                                    'Raster9_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': SZE,
                                                                                    'Raster10_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster11': TD,
                                                                                    'Raster11_rasterInfo':{'pixelType':10,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }
            elif (itemURI['tag'] == "QA"):
                #QA
                bandProperties = [{'bandName':'fmask'},
                                    {'bandName':'lambertian_contiguity'},
                                    {'bandName':'nbar_contiguity'},
                                    {'bandName':'nbart_contiguity'},
                                    {'bandName':'terrain_shadow'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"GS_QA_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': FM,
                                                                                    'Raster1_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': L03,
                                                                                    'Raster2_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NR03,
                                                                                    'Raster3_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NRT03,
                                                                                    'Raster4_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': TS,
                                                                                    'Raster5_rasterInfo':{'pixelType':3,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

            elif (itemURI['tag'] == "Lambertian"):
                #Lambertian
                bandProperties = [{'bandName':'lambertian_blue'},
                                    {'bandName':'lambertian_coastal_aerosol'},
                                    {'bandName':'lambertian_green'},
                                    {'bandName':'lambertian_red'},
                                    {'bandName':'lambertian_red_edge_1'},
                                    {'bandName':'lambertian_red_edge_2'},
                                    {'bandName':'lambertian_red_edge_3'},
                                    {'bandName':'lambertian_nir_1'},
                                    {'bandName':'lambertian_nir_2'},
                                    {'bandName':'lambertian_swir_2'},
                                    {'bandName':'lambertian_swir_3'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"GS_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': L02,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': L01,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': L04,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': L07,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': L08,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': L09,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': L10,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': L05,
                                                                                    'Raster8_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': L06,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': L11,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster11': L12,
                                                                                    'Raster11_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }


            elif (itemURI['tag'] == "NBAR"):
                #NBAR
                bandProperties = [{'bandName':'nbar_blue'},
                                    {'bandName':'nbar_coastal_aerosol'},
                                    {'bandName':'nbar_green'},
                                    {'bandName':'nbar_red'},
                                    {'bandName':'nbar_red_edge_1'},
                                    {'bandName':'nbar_red_edge_2'},
                                    {'bandName':'nbar_red_edge_3'},
                                    {'bandName':'nbar_nir_1'},
                                    {'bandName':'nbar_nir_2'},
                                    {'bandName':'nbar_swir_2'},
                                    {'bandName':'nbar_swir_3'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"GS_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NR02,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NR01,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NR04,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NR07,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': NR08,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': NR09,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': NR10,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': NR05,
                                                                                    'Raster8_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': NR06,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': NR11,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster11': NR12,
                                                                                    'Raster11_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

            elif (itemURI['tag'] == "NBART"):
                #NBART
                bandProperties = [{'bandName':'nbart_blue'},
                                    {'bandName':'nbart_coastal_aerosol'},
                                    {'bandName':'nbart_green'},
                                    {'bandName':'nbart_red'},
                                    {'bandName':'nbart_red_edge_1'},
                                    {'bandName':'nbart_red_edge_2'},
                                    {'bandName':'nbart_red_edge_3'},
                                    {'bandName':'nbart_nir_1'},
                                    {'bandName':'nbart_nir_2'},
                                    {'bandName':'nbart_swir_1'},
                                    {'bandName':'nbart_swir_2'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"GS_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NRT02,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':1830,'nRows':1830,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NRT01,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NRT04,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NRT07,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': NRT08,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': NRT09,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': NRT10,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': NRT05,
                                                                                    'Raster8_rasterInfo':{'pixelType':6,'ncols':10980,'nRows':10980,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': NRT06,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': NRT11,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster11': NRT12,
                                                                                    'Raster11_rasterInfo':{'pixelType':6,'ncols':5490,'nRows':5490,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

            cordsList = doc['grid_spatial']['projection']['valid_data']['coordinates']
################### Assemble everything into an outgoing dictionary
            metadata['bandProperties'] = bandProperties
            builtItem['spatialReference'] = srsWKT
            builtItem['variables'] = variables
            builtItem['itemUri'] = itemURI
            builtItem['keyProperties'] = metadata
            builtItem['footprint'] = cordsList[0]
            builtItemsList = list()
            builtItemsList.append(builtItem)
            return builtItemsList
        except Exception as e:
            raise
        return None

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Geoscience Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
class GeoscienceSentinelCrawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()
        try:
            self.paths = crawlerProperties['paths']
            self.recurse = crawlerProperties['recurse']
            self.filter = crawlerProperties['filter']
        except:
##            print ('Error in crawler properties')
            return None
        self.run = 1
        if (self.filter is (None or "")):
            self.filter = 'L2*METADATA.yaml'
        try:
            self.pathGenerator = self.createGenerator()
        except StopIteration:
            return None

        try:
            self.tagGenerator = self.createTagGenerator()   #reinitialize tag generator
        except StopIteration:
            return None

    def createTagGenerator(self):
        for tag in ["MS","Supplementary","Lambertian","QA","NBART","NBAR"]:      #Landsat8 L2 product have 5 types of sub-products
            yield tag

    def createGenerator(self):
        for path in self.paths:
            if (path.startswith("http") or (path.startswith("s3"))):
                yield path

            elif (not os.path.exists(path)):
                continue

            elif (os.path.isdir(path)):
                if (self.recurse):
                    for root, dirs, files in (os.walk(path)):
                        for file in (files):
                            if (file.endswith(".yaml")):
                                filename = os.path.join(root, file)
                                yield filename
                else:
                    filter_to_scan = path + os.path.sep + self.filter
                    for filename in glob.glob(filter_to_scan):
                        yield filename

            elif (path.endswith(".csv")):
                with open(path, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    rasterFieldIndex = -1
                    firstRow = next(reader)
                    #Check for the 'raster' field in the csv file, if not present take the first field as input data
                    for attribute in firstRow:
                        if (attribute.lower() == 'raster'):
                            rasterFieldIndex = firstRow.index(attribute)
                            break
                    if (rasterFieldIndex == -1):
                        csvfile.seek(0)
                        rasterFieldIndex = 0
                    for row in reader:
                        filename = row[rasterFieldIndex]
                        if (filename.startswith("http")or (filename.startswith("s3"))):   #if the csv list contains a list of s3 urls
                            yield filename
                        elif (filename.endswith(".yaml") and os.path.exists(filename)):
                            yield filename
            elif (path.endswith(".yaml")):
                yield path

    def __iter__(self):
        return self

    def next(self):
        ## Return URI dictionary to Builder
        return self.getNextUri()

    def getNextUri(self):
        try:
            if (self.run ==1):
                try:
                    self.curPath = next(self.pathGenerator)
                    self.run=10
                except:
                    return None
            if ((self.curPath).startswith("http:")):
                doc = self.utils.readYamlS3(self.curPath)
            elif ((self.curPath).startswith("s3:")):
                _yamlpath = self.curPath
                index = _yamlpath.find("/",5) #giving a start index of 5 will ensure that the / from s3:// is not returned.
                bucketname = _yamlpath[5:index] #First 5 letters will always be s3://
                key = _yamlpath[index+1:]
                doc = self.utils.readYamlS3_boto3(bucketname,key)
            else:
                doc = self.utils.readYaml(self.curPath)
            productName = self.utils.getProductName(doc)
            processingLevel = self.utils.getProcessingLevel(doc)
            if (processingLevel=="Level-2"):
                try:
                    curTag = next(self.tagGenerator)
                except StopIteration:
                    try:
                        self.tagGenerator = self.createTagGenerator()   #reinitialize tag generator
                    except StopIteration:
                        return None
                    try:
                        self.curPath = next(self.pathGenerator)
                    except:
                        return None
                    curTag = next(self.tagGenerator)
                    if ((self.curPath).startswith("http:")):        #this is needed to get the product name from the new path
                        doc = self.utils.readYamlS3(self.curPath)
                    else:
                        doc = self.utils.readYaml(self.curPath)
                    productName = self.utils.getProductName(doc)

            else:
                self.curPath = next(self.pathGenerator)
                curTag = "MS"
        except StopIteration:
            return None
        uri = {
                'path': self.curPath,
                'displayName': os.path.split(os.path.dirname(self.curPath))[1],
                'tag': curTag,
                'groupName': os.path.split(os.path.dirname(self.curPath))[1],
                'productName':productName
              }
        return uri
