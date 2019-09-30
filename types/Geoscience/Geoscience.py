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
# Name: Geoscience
# Description: Geoscience python raster type.
# Version: 20190925
# Requirements: python.exe v2.7 and above, standard python libraries, python yaml library(https://pyyaml.org/wiki/PyYAMLDocumentation), ArcMap , python requests module , python boto3 module, python gdal module
# Required Arguments:  N/A
# Optional Arguments: N/A
# Usage: Used through ArcMap/ArcPro as python raster type.
# Author: Esri Imagery Workflows Team
# ------------------------------------------------------------------------------

import os
import arcpy
import glob
import csv
##import urllib.request
import requests
from osgeo import gdal

try:
    import yaml
    import boto3
except ImportError as e:
    print ('Err. {}'.format(str(e)))
    exit(1)

class DataSourceType():
    File = 1
    Folder = 2

class RasterTypeFactory():

    def getRasterTypesInfo(self):
        self.dacq_auxField = arcpy.Field()              #Creation Date
        self.dacq_auxField.name = 'AcquisitionDate'
        self.dacq_auxField.aliasName = 'Acquisition Date'
        self.dacq_auxField.type = 'date'

        self.platform_auxField = arcpy.Field()
        self.platform_auxField.name = 'Platform'
        self.platform_auxField.aliasName = 'Platform'
        self.platform_auxField.type = 'String'
        self.platform_auxField.length = 50

        self.instrument_auxField = arcpy.Field()
        self.instrument_auxField.name = 'Instrument'
        self.instrument_auxField.aliasName = 'Instrument'
        self.instrument_auxField.type = 'String'
        self.instrument_auxField.length = 50

        self.prodtype_auxField = arcpy.Field()
        self.prodtype_auxField.name = 'ProductType'
        self.prodtype_auxField.aliasName = 'Product Type'
        self.prodtype_auxField.type = 'String'
        self.prodtype_auxField.length = 50

        self.id_auxField = arcpy.Field()
        self.id_auxField.name = 'ID'
        self.id_auxField.aliasName = 'ID'
        self.id_auxField.type = 'String'
        self.id_auxField.length = 50

        return [
                {
                    'rasterTypeName': 'Geoscience',
                    'builderName': 'GeoscienceBuilder',
                    'description': ("Supports reading of Geoscience Landsat8 data"),
                    'supportsOrthorectification': False,
                    'enableClipToFootprint': True,
                    'isRasterProduct': False,
                    'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                    'dataSourceFilter': '*.yaml',   # if the input raster is a raster proxy then use'*.yaml;RasterProxy'
                    'crawlerName': 'GeoscienceCrawler',
                    'productDefinitionName': 'Geoscience',
                    'processingTemplates': [
                                            {
                                                'name': 'Geoscience_MS_be',
                                                'enabled': False,
                                                'outputDatasetTag': 'be',
                                                'primaryInputDatasetTag': 'be',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_be.rft.xml'
                                            } ,
                                            {
                                                'name': 'Geoscience_MS_fc',
                                                'enabled': True,
                                                'outputDatasetTag': 'fc',
                                                'primaryInputDatasetTag': 'fc',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_fc.rft.xml'
                                            }
                                           , {
                                                'name': 'Geoscience_MS_mc',
                                                'enabled': False,
                                                'outputDatasetTag': 'mc',
                                                'primaryInputDatasetTag': 'mc',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_mc.rft.xml'
                                            },
                                            {
                                                'name': 'Geoscience_MS_wofs_fs',
                                                'enabled': False,
                                                'outputDatasetTag': 'wofs_fs',
                                                'primaryInputDatasetTag': 'wofs_fs',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_wofs_fs.rft.xml'
                                            },
                                            {
                                                'name': 'Geoscience_MS_wofs_ss',
                                                'enabled': False,
                                                'outputDatasetTag': 'wofs_ss',
                                                'primaryInputDatasetTag': 'wofs_ss',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_wofs_ss.rft.xml'
                                            },
                                            {
                                                'name': 'Geoscience_MS_wofs',
                                                'enabled': False,
                                                'outputDatasetTag': 'wofs',
                                                'primaryInputDatasetTag': 'wofs',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'Geoscience_MS_wofs.rft.xml'
                                            }
                                           ],
                    #GET THE CORRECT BAND INDEX , MIN MAX WAVELENGTH
                    'bandProperties': [
                                        {
                                            'bandName': 'blue',
                                            'bandIndex': 1,
                                            'wavelengthMin': 452.0,
                                            'wavelengthMax': 512.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'green',
                                            'bandIndex': 2,
                                            'wavelengthMin': 533.0,
                                            'wavelengthMax': 590.0,
                                            'datasetTag': 'MS'
                                        },
                                         {
                                            'bandName': 'nir',
                                            'bandIndex': 4,
                                            'wavelengthMin': 851.0,
                                            'wavelengthMax': 879.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'red',
                                            'bandIndex': 3,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'swir1',
                                            'bandIndex': 5,
                                            'wavelengthMin': 1566.0,
                                            'wavelengthMax': 1651.0,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'swir2',
                                            'bandIndex': 6,
                                            'wavelengthMin': 2107.0,
                                            'wavelengthMax': 2294.0,
                                            'datasetTag': 'SWIR'
                                        },
                                        {
                                            'bandName': 'BS',
                                            'bandIndex': 0,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'NPV',
                                            'bandIndex': 1,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'PV',
                                            'bandIndex': 2,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'UE',
                                            'bandIndex': 3,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        }
                                        ,{
                                            'bandName': 'canopy_cover_class',
                                            'bandIndex': 0,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'extent',
                                           'bandIndex': 1,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        } ,
                                        {
                                            'bandName': 'confidence',
                                            'bandIndex': 0,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'wofs_filtered_summary',
                                            'bandIndex': 1,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        } ,
                                        {
                                            'bandName': 'count_clear',
                                            'bandIndex': 0,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'count_wet',
                                           'bandIndex': 1,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        },
                                        {
                                            'bandName': 'frequency',
                                           'bandIndex': 2,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        }  ,
                                        {
                                            'bandName': 'water',
                                           'bandIndex': 0,
                                            'wavelengthMin': 636.0,
                                            'wavelengthMax': 673.0,
                                            'datasetTag': 'MS'
                                        }
                                      ],

                    'fields': [self.dacq_auxField,
                               self.platform_auxField,
                               self.instrument_auxField,
                               self.prodtype_auxField,
                               self.id_auxField]
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
                    print(exc)
                    return None
                return doc
        except:
            print ("Error in opening yaml file")
            return None

##    def readYamlS3(self,path):          #to read the yaml file located on S3
##        page= urllib.request.urlopen(path)
##        try:
##            doc= (yaml.load(page))
##        except yaml.YAMLError as exc:
##            print(exc)
##            return None
##        return doc

    def readYamlS3_boto3(self,bucket,path):          #to read the yaml file located on S3 over s3:// protocol
        client = boto3.client('s3')
        try:
            page = client.get_object(Bucket=bucket,Key=path,RequestPayer='requester')
            page = client.get_object(Bucket=bucket,Key=path)
            doc = (yaml.load(page['Body'].read()))
        except yaml.YAMLError as exc:
            print(exc)
            return None
        return doc


    def readYamlS3(self,path):          #to read the yaml file located on S3 over https:// protocol
        page=requests.get(path,stream=True,timeout=None)
        try:
            doc= (yaml.load(page.content))
        except yaml.YAMLError as exc:
            print(exc)
            return None
        return doc


    def getProductName(self, path):
        path=os.path.basename(path)
        if (path.startswith('be')):
            productName='landsat8_barest_earth_mosaic'
        elif ('FC' in path):
            productName='fractional_cover'
        elif (path.startswith('MANGROVE_COVER')):
            productName='mangrove_extent_cover'
        elif (path.startswith('wofs_filtered_summary')):
            productName='wofs_filtered_summary'
        elif (path.endswith('summary.yaml')):
            productName='wofs_statistical_summary'
        elif (path.startswith('LS_WATER') or path.startswith('WATER')):
            productName = 'wofs'

        return productName
        return None


    def getTag(self, productName):
        if productName=='landsat8_barest_earth_mosaic':
            tag= 'be'
        elif productName=='fractional_cover' :
            tag='fc'
        elif productName=='mangrove_extent_cover':
            tag='mc'
        elif productName=='wofs_filtered_summary':
            tag='wofs_fs'
        elif productName=='wofs_statistical_summary':
            tag='wofs_ss'
        elif productName == 'wofs':
            tag= 'wofs'
        return tag

        return None



# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Geoscience builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class GeoscienceBuilder():

    def __init__(self, **kwargs):
        arcpy.AddMessage("BuilderClass")
        self.SensorName = 'Geoscience'
        self.utils = Utilities()

    def canOpen(self, datasetPath):
        return True


    def embedMRF(self,inputDir,fileName,maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,nBands,protocol,cachepath):

        try:
            #create template for 10m resolution and write to a file
            cachingMRF = \
                '<MRF_META>\n'  \
                '  <CachedSource>\n'  \
                '    <Source>/{13}{0}/{1}</Source>\n'  \
                '  </CachedSource>\n'  \
                '  <Raster>\n'  \
                '    <Size c="{12}" x="{8}" y="{9}"/>\n'  \
                '    <PageSize c="1" x="512" y="512"/>\n'  \
                '    <Compression>LERC</Compression>\n'  \
                '    <DataType>{10}</DataType>\n'  \
                '  <DataFile>z:/mrfcache/Geoscience/{14}/{7}.mrf_cache</DataFile><IndexFile>z:/mrfcache/Geoscience/{14}/{7}.mrf_cache</IndexFile></Raster>\n'  \
                '  <Rsets model="uniform" scale="2"/>\n'  \
                '  <GeoTags>\n'  \
                '    <BoundingBox maxx="{2}" maxy="{3}" minx="{4}" miny="{5}"/>\n'  \
                '    <Projection>{6}</Projection>\n'  \
                '  </GeoTags>\n'  \
                '  <Options>V2=ON</Options>\n'  \
                '</MRF_META>'.format(inputDir,fileName,maxX,maxY,minX,minY,prjString,fileName[0:-4],nCols,nRows,dataType,tag,nBands,protocol,cachepath)

        except Exception as exp:
            arcpy.AddMessage(str(exp))
        return cachingMRF

    # Fractional_cover embedded mrf should not have <data_type> line, hence a separate method
    def embedMRF_fc(self,inputDir,fileName,maxX,maxY,minX,minY,prjString,nRows,nCols,tag,nBands,protocol,cachepath):
        try:
            #create template for 10m resolution and write to a file
            cachingMRF = \
                '<MRF_META>\n'  \
                '  <CachedSource>\n'  \
                '    <Source>/{12}{0}/{1}</Source>\n'  \
                '  </CachedSource>\n'  \
                '  <Raster>\n'  \
                '    <Size c="{11}" x="{8}" y="{9}"/>\n'  \
                '    <PageSize c="1" x="512" y="512"/>\n'  \
                '    <Compression>LERC</Compression>\n'  \
                '  <DataFile>z:/mrfcache/Geoscience/{13}/{7}.mrf_cache</DataFile><IndexFile>z:/mrfcache/Geoscience/{13}/{7}.mrf_cache</IndexFile></Raster>\n'  \
                '  <Rsets model="uniform" scale="2"/>\n'  \
                '  <GeoTags>\n'  \
                '    <BoundingBox maxx="{2}" maxy="{3}" minx="{4}" miny="{5}"/>\n'  \
                '    <Projection>{6}</Projection>\n'  \
                '  </GeoTags>\n'  \
                '  <Options>V2=ON</Options>\n'  \
                '</MRF_META>'.format(inputDir,fileName,maxX,maxY,minX,minY,prjString,fileName[0:-4],nCols,nRows,tag,nBands,protocol,cachepath)

        except Exception as exp:
            arcpy.AddMessage(str(exp))
        return cachingMRF

    #Fetch the rows,cols
    def returnVals(self,path):
        try:
            data = gdal.Open(path)
            X = data.RasterXSize
            Y = data.RasterYSize
            return X,Y
        except Exception as exp:
            arcpy.AddMessage(str(exp))

    def readNembedMRF(self,path):
        try:
            openFile = open(path,"r" )
            cachingMRF=""
            for line in openFile.readlines():
                cachingMRF  = cachingMRF + line
            return cachingMRF
        except Exception as exp:
            arcpy.AddMessage(str(exp))

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

                tag= itemURI['tag']

                if tag=='be':
                    beFlag=0
                    nRows=1000
                    nCols=1000
                    noBands= 6
                    dataType='Int16'
                    pixelType= 6

                    yamldir= os.path.dirname(_yamlpath)

                    if (_yamlpath.startswith("http")):
                        doc = self.utils.readYamlS3(_yamlpath)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False

                        lastIdx= _yamlpath.rfind('/')
                        startIdx= _yamlpath.find('.com')+5
                        inputDir= _yamlpath[startIdx:lastIdx]

                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        protocol ='vsicurl/http://'
                        cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")].replace(".s3.amazonaws.com","")

                        bluePath= doc['image']['bands']['blue']['path']
                        greenPath= doc['image']['bands']['green']['path']

                        if bluePath== greenPath:
                            prjString= doc['grid_spatial']['projection']['spatial_reference']

                            BE = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)

                        else:
                            beFlag=1

                            spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                            spatialIdx= spatialRef.find(':')
                            spatialId= int(spatialRef[spatialIdx+1:])
                            prjString= arcpy.SpatialReference(spatialId).exportToString()
                            srsEPSG= spatialId

                            BE1 = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE2 = self.embedMRF(inputDir,(doc['image']['bands']['green']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE3 = self.embedMRF(inputDir,(doc['image']['bands']['red']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE4 = self.embedMRF(inputDir,(doc['image']['bands']['nir']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE5 = self.embedMRF(inputDir,(doc['image']['bands']['swir1']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE6 = self.embedMRF(inputDir,(doc['image']['bands']['swir2']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)


                    elif (_yamlpath.startswith("s3:")):
                        index = _yamlpath.find("/",5) #giving a start index of 5 will ensure that the / from s3:// is not returned.
                        bucketname = _yamlpath[5:index] #First 5 letters will always be s3://
                        key = _yamlpath[index+1:]
                        doc = self.utils.readYamlS3_boto3(bucketname,key)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False
                        lastIdx= _yamlpath.rfind('/')
                        inputDir= _yamlpath[5:lastIdx]  #along with the bucket name

                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        protocol ='vsis3/'
                        cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")]

                        bluePath= doc['image']['bands']['blue']['path']
                        greenPath= doc['image']['bands']['green']['path']

                        if bluePath== greenPath:
                            prjString= doc['grid_spatial']['projection']['spatial_reference']

                            BE = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)

                        else:
                            beFlag=1

                            spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                            spatialIdx= spatialRef.find(':')
                            spatialId= int(spatialRef[spatialIdx+1:])
                            prjString= arcpy.SpatialReference(spatialId).exportToString()
                            srsEPSG= spatialId

                            BE1 = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE2 = self.embedMRF(inputDir,(doc['image']['bands']['green']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE3 = self.embedMRF(inputDir,(doc['image']['bands']['red']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE4 = self.embedMRF(inputDir,(doc['image']['bands']['nir']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE5 = self.embedMRF(inputDir,(doc['image']['bands']['swir1']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            BE6 = self.embedMRF(inputDir,(doc['image']['bands']['swir2']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)


                    else:
                        doc = self.utils.readYaml(_yamlpath)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False

                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        bluePath= doc['image']['bands']['blue']['path']
                        greenPath= doc['image']['bands']['green']['path']

                        if bluePath== greenPath:
                            prjString= doc['grid_spatial']['projection']['spatial_reference']

                            BE = os.path.join(yamldir,(doc['image']['bands']['blue']['path']))

                        else:
                            beFlag=1

                            spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                            spatialIdx= spatialRef.find(':')
                            spatialId= int(spatialRef[spatialIdx+1:])
                            prjString= arcpy.SpatialReference(spatialId).exportToString()
                            srsEPSG= spatialId

                            BE1 = os.path.join(yamldir,(doc['image']['bands']['blue']['path']))
                            nCols,nRows = self.returnVals(BE1)

                            BE2 = os.path.join(yamldir,(doc['image']['bands']['green']['path']))
                            nCols,nRows = self.returnVals(BE2)

                            BE3 = os.path.join(yamldir,(doc['image']['bands']['red']['path']))
                            nCols,nRows = self.returnVals(BE3)

                            BE4 = os.path.join(yamldir,(doc['image']['bands']['nir']['path']))
                            nCols,nRows = self.returnVals(BE4)

                            BE5 = os.path.join(yamldir,(doc['image']['bands']['swir1']['path']))
                            nCols,nRows = self.returnVals(BE5)

                            BE6 = os.path.join(yamldir,(doc['image']['bands']['swir2']['path']))
                            nCols,nRows = self.returnVals(BE6)

                            if itemURI['uriProperties']['rpflag'] ==1:               #if the files on disk are raster proxies.
                                BE1 = self.readNembedMRF(BE1)
                                BE2 = self.readNembedMRF(BE2)
                                BE3 = self.readNembedMRF(BE3)
                                BE4 = self.readNembedMRF(BE4)
                                BE5 = self.readNembedMRF(BE5)
                                BE6 = self.readNembedMRF(BE6)

                else:
                    # here noBands refers to the number of bands present in each tiff being used
                    if tag=='fc':
                        nRows=4000
                        nCols=4000
                        noBands= 1
                        dataType='Int8'
                        pixelType= 4

                    elif tag=='mc':
                        nRows=4000
                        nCols=4000
                        noBands=1
                        dataType='Int16'
                        pixelType=6

                    elif tag=='wofs_fs':
                        nRows=4000
                        nCols=4000
                        noBands=1
                        dataType='Float32'
                        pixelType= 9

                    elif tag=='wofs_ss':
                        nRows=4000
                        nCols=4000
                        noBands=1
                        # different bands have different data type, hence not defined


                    elif tag=='wofs':
                        nRows=4000
                        nCols=4000
                        noBands=1
                        dataType= 'Int8'
                        pixelType=3

                    yamldir= os.path.dirname(_yamlpath)

                    if (_yamlpath.startswith("http")):
                        doc = self.utils.readYamlS3(_yamlpath)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False

                        lastIdx= _yamlpath.rfind('/')
                        startIdx= _yamlpath.find('.com')+5
                        inputDir= _yamlpath[startIdx:lastIdx]

                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        protocol ='vsicurl/http://'
                        cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")].replace(".s3.amazonaws.com","")

                        spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                        spatialIdx= spatialRef.find(':')
                        spatialId= int(spatialRef[spatialIdx+1:])
                        prjString= arcpy.SpatialReference(spatialId).exportToString()
                        srsEPSG= spatialId


                        if tag=='fc':
                            FC01 = self.embedMRF_fc(inputDir,(doc['image']['bands']['BS']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC02 = self.embedMRF_fc(inputDir,(doc['image']['bands']['NPV']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC03 = self.embedMRF_fc(inputDir,(doc['image']['bands']['PV']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC04 = self.embedMRF_fc(inputDir,(doc['image']['bands']['UE']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                        elif tag=='mc':
                            MC01= self.embedMRF(inputDir,(doc['image']['bands']['canopy_cover_class']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            MC02= self.embedMRF(inputDir,(doc['image']['bands']['extent']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                        elif tag=='wofs_fs':
                            WFS01= self.embedMRF(inputDir,(doc['image']['bands']['confidence']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            WFS02= self.embedMRF(inputDir,(doc['image']['bands']['wofs_filtered_summary']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                        elif tag=='wofs_ss':
                            WSS01= self.embedMRF(inputDir,(doc['image']['bands']['count_clear']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Int16',tag,noBands,protocol,cachepath)
                            WSS02= self.embedMRF(inputDir,(doc['image']['bands']['count_wet']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Int16',tag,noBands,protocol,cachepath)
                            WSS03= self.embedMRF(inputDir,(doc['image']['bands']['frequency']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Float32',tag,noBands,protocol,cachepath)
                        elif tag=='wofs':
                            W01= self.embedMRF(inputDir,(doc['image']['bands']['water']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            endIndex = (doc['image']['bands']['water']['path']).rfind("_")
                            fileName = doc['image']['bands']['water']['path'][0:endIndex]   #the display name in the itemURI will be changed to this.

                    elif (_yamlpath.startswith("s3:")):
                        index = _yamlpath.find("/",5) #giving a start index of 5 will ensure that the / from s3:// is not returned.
                        bucketname = _yamlpath[5:index] #First 5 letters will always be s3://
                        key = _yamlpath[index+1:]
                        doc = self.utils.readYamlS3_boto3(bucketname,key)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False
                        lastIdx= _yamlpath.rfind('/')
                        inputDir= _yamlpath[5:lastIdx]  #along with the bucket name

                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        protocol ='vsis3/'
                        cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")]

                        spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                        spatialIdx= spatialRef.find(':')
                        spatialId= int(spatialRef[spatialIdx+1:])
                        prjString= arcpy.SpatialReference(spatialId).exportToString()
                        srsEPSG= spatialId


                        if tag=='fc':
                            FC01 = self.embedMRF_fc(inputDir,(doc['image']['bands']['BS']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC02 = self.embedMRF_fc(inputDir,(doc['image']['bands']['NPV']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC03 = self.embedMRF_fc(inputDir,(doc['image']['bands']['PV']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                            FC04 = self.embedMRF_fc(inputDir,(doc['image']['bands']['UE']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,tag,noBands,protocol,cachepath)
                        elif tag=='mc':
                            MC01= self.embedMRF(inputDir,(doc['image']['bands']['canopy_cover_class']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            MC02= self.embedMRF(inputDir,(doc['image']['bands']['extent']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                        elif tag=='wofs_fs':
                            WFS01= self.embedMRF(inputDir,(doc['image']['bands']['confidence']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            WFS02= self.embedMRF(inputDir,(doc['image']['bands']['wofs_filtered_summary']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                        elif tag=='wofs_ss':
                            WSS01= self.embedMRF(inputDir,(doc['image']['bands']['count_clear']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Int16',tag,noBands,protocol,cachepath)
                            WSS02= self.embedMRF(inputDir,(doc['image']['bands']['count_wet']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Int16',tag,noBands,protocol,cachepath)
                            WSS03= self.embedMRF(inputDir,(doc['image']['bands']['frequency']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,'Float32',tag,noBands,protocol,cachepath)
                        elif tag=='wofs':
                            W01= self.embedMRF(inputDir,(doc['image']['bands']['water']['path']),maxX,maxY,minX,minY,prjString,nRows,nCols,dataType,tag,noBands,protocol,cachepath)
                            endIndex = (doc['image']['bands']['water']['path']).rfind("_")
                            fileName = doc['image']['bands']['water']['path'][0:endIndex]   #the display name in the itemURI will be changed to this.

                    else:

                        doc = self.utils.readYaml(_yamlpath)
                        if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                            print  ('Err. Invalid input format!')
                            return False

                        yamldir= os.path.dirname(_yamlpath)
                        refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                        maxX= refPoints['lr']['x']
                        maxY= refPoints['ur']['y']
                        minX= refPoints['ll']['x']
                        minY= refPoints['ll']['y']

                        spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                        spatialIdx= spatialRef.find(':')
                        spatialId= int(spatialRef[spatialIdx+1:])
                        prjString= arcpy.SpatialReference(spatialId).exportToString()
                        srsEPSG= spatialId

                        if tag=='fc':
                            FC01 = os.path.join(yamldir,(doc['image']['bands']['BS']['path']))
                            nCols,nRows = self.returnVals(FC01)
                            FC02 = os.path.join(yamldir,(doc['image']['bands']['NPV']['path']))
                            nCols,nRows = self.returnVals(FC02)
                            FC03 = os.path.join(yamldir,(doc['image']['bands']['PV']['path']))
                            nCols,nRows = self.returnVals(FC03)
                            FC04 = os.path.join(yamldir,(doc['image']['bands']['UE']['path']))
                            nCols,nRows = self.returnVals(FC04)

                            if itemURI['uriProperties']['rpflag'] == 1:                  #if the files on disk are raster proxies.
                                FC01 = self.readNembedMRF(FC01)
                                FC02 = self.readNembedMRF(FC02)
                                FC03 = self.readNembedMRF(FC03)
                                FC04 = self.readNembedMRF(FC04)
                        elif tag=='mc':
                            MC01= os.path.join(yamldir,(doc['image']['bands']['canopy_cover_class']['path']))
                            nCols,nRows = self.returnVals(MC01)

                            MC02= os.path.join(yamldir,(doc['image']['bands']['extent']['path']))
                            nCols,nRows = self.returnVals(MC02)

                            if itemURI['uriProperties']['rpflag'] == 1:                  #if the files on disk are raster proxies.
                                MC01 = self.readNembedMRF(MC01)
                                MC02 = self.readNembedMRF(MC02)
                        elif tag=='wofs_fs':
                            WFS01= os.path.join(yamldir,(doc['image']['bands']['confidence']['path']))
                            nCols,nRows = self.returnVals(WFS01)

                            WFS02= os.path.join(yamldir,(doc['image']['bands']['wofs_filtered_summary']['path']))
                            nCols,nRows = self.returnVals(WFS02)

                            if itemURI['uriProperties']['rpflag'] == 1:                  #if the files on disk are raster proxies.
                                WFS01 = self.readNembedMRF(WFS01)
                                WFS02 = self.readNembedMRF(WFS02)
                        elif tag=='wofs_ss':
                            WSS01= os.path.join(yamldir,(doc['image']['bands']['count_clear']['path']))
                            nCols,nRows = self.returnVals(WSS01)

                            WSS02= os.path.join(yamldir,(doc['image']['bands']['count_wet']['path']))
                            nCols,nRows = self.returnVals(WSS02)

                            WSS03= os.path.join(yamldir,(doc['image']['bands']['frequency']['path']))
                            nCols,nRows = self.returnVals(WSS03)

                            if itemURI['uriProperties']['rpflag'] == 1:                  #if the files on disk are raster proxies.
                                WSS01 = self.readNembedMRF(WSS01)
                                WSS02 = self.readNembedMRF(WSS02)
                                WSS03 = self.readNembedMRF(WSS03)
                        elif tag=='wofs':
                            W01= os.path.join(yamldir,(doc['image']['bands']['water']['path']))
                            nCols,nRows = self.returnVals(W01)
                            if itemURI['uriProperties']['rpflag'] == 1:                  #if the files on disk are raster proxies.
                                W01 = self.readNembedMRF(W01)
                            endIndex = (doc['image']['bands']['water']['path']).rfind("_")
                            fileName = doc['image']['bands']['water']['path'][0:endIndex]   #the display name in the itemURI will be changed to this.


                #wofs_fs does not have a different footprint provided
                if (tag!='wofs_fs'):
                    vertex_array = arcpy.Array()
                    try:
                        coords= doc['grid_spatial']['projection']['valid_data']['coordinates'][0]
                    except:
                        coords= None
                    if coords:
                        for point in coords:
                            vertex_array.add(arcpy.Point(point[0],point[1]))

                        footprint_geometry = arcpy.Polygon(vertex_array,srsEPSG)


                #Metadata Information
                metadata = {}
                instrument = doc['instrument']['name']
                if (instrument is not None):
                    metadata['Instrument']=instrument

                platform = doc['platform']['code']
                if (platform is not None):
                    metadata['Platform']=platform

                prodtype = doc['product_type']
                if (prodtype is not None):
                    metadata['ProductType']=prodtype

                id = doc['id']
                if (doc is not None):
                    metadata['ID']=id

                acqdate = doc['extent']['center_dt']
                if (acqdate is not None):
                    metadata['AcquisitionDate']= acqdate[0:19].replace("T"," ")

                builtItem = {}

                if (tag == "be"):
                    bandProperties = [{'bandName':'blue'},
                                        {'bandName':'green'},
                                        {'bandName':'red'},
                                        {'bandName':'nir'},
                                        {'bandName':'swir1'},
                                        {'bandName':'swir2'}]

                    if beFlag==0:

                        rasterInfo={}
                        rasterInfo['pixelType']= pixelType
                        rasterInfo['nCols']= nCols
                        rasterInfo['nRows']= nRows
                        rasterInfo['nBands']= noBands
                        rasterInfo['spatialReference']= prjString
                        rasterInfo['XMin']= minX
                        rasterInfo['YMin']= minY
                        rasterInfo['XMax']= maxX
                        rasterInfo['YMax']= maxY

                        builtItem['raster'] = {'uri': BE , 'rasterInfo': rasterInfo}
                        builtItem['spatialReference'] = prjString

                        metadata['bandProperties'] = bandProperties

                    else:
                        builtItem['raster']={'functionDataset':{
                                                            'rasterFunction':"GS_Composite_be.rft.xml",
                                                            'rasterFunctionArguments':{
                                                                                        'Raster1': BE1,
                                                                                        'Raster1_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},

                                                                                        'Raster2': BE2,
                                                                                        'Raster2_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster3': BE3,
                                                                                        'Raster3_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster4': BE4,
                                                                                        'Raster4_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster5': BE5,
                                                                                        'Raster5_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster6': BE6,
                                                                                        'Raster6_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY}
                                                                                       }
                                                            }
                                        }

                        metadata['bandProperties'] = bandProperties
                        try:
                            builtItem['footprint']= footprint_geometry
                        except :
                            arcpy.AddMessage("No valid geometry to update the footprint")
                        builtItem['spatialReference'] = srsEPSG

                elif (tag== 'fc'):
                    bandProperties = [{'bandName':'BS'},
                                        {'bandName':'NPV'},
                                        {'bandName':'PV'},
                                        {'bandName':'UE'}]


                    builtItem['raster'] ={'functionDataset':{
                                                            'rasterFunction':"GS_Composite_fc.rft.xml",
                                                            'rasterFunctionArguments':{
                                                                                        'Raster1': FC01,
                                                                                        'Raster1_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},

                                                                                        'Raster2': FC02,
                                                                                        'Raster2_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster3': FC03,
                                                                                        'Raster3_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},
                                                                                        'Raster4': FC04,
                                                                                        'Raster4_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY}
                                                                                       }
                                                            }
                                        }

                    metadata['bandProperties'] = bandProperties
                    try:
                        builtItem['footprint']= footprint_geometry
                    except :
                        arcpy.AddMessage("No valid geometry to update the footprint")
                    builtItem['spatialReference'] = srsEPSG


                elif (tag=='mc'):
                    bandProperties = [{'bandName':'canopy_cover_class'},
                                        {'bandName':'extent'}]


                    builtItem['raster'] ={'functionDataset':{
                                                            'rasterFunction':"GS_Composite_mc.rft.xml",
                                                            'rasterFunctionArguments':{
                                                                                        'Raster1': MC01,
                                                                                        'Raster1_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},

                                                                                        'Raster2': MC02,
                                                                                        'Raster2_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY}
                                                                                       }
                                                            }
                                        }

                    metadata['bandProperties'] = bandProperties
                    try:
                        builtItem['footprint']= footprint_geometry
                    except :
                        arcpy.AddMessage("No valid geometry to update the footprint")
                    builtItem['spatialReference'] = srsEPSG

                elif (tag=='wofs_fs'):
                    bandProperties = [{'bandName':'confidence'},
                                        {'bandName':'wofs_filtered_summary'}]


                    builtItem['raster'] ={'functionDataset':{
                                                            'rasterFunction':"GS_Composite_wofs_fs.rft.xml",
                                                            'rasterFunctionArguments':{
                                                                                        'Raster1': WFS01,
                                                                                        'Raster1_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},

                                                                                        'Raster2': WFS02,
                                                                                        'Raster2_info':{'pixelType':pixelType,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY}
                                                                                       }
                                                            }
                                        }

                elif (tag=='wofs_ss'):
                    bandProperties = [{'bandName':'count_clear'},
                                        {'bandName':'count_wet'},
                                        {'bandName':'frequency'}]


                    builtItem['raster'] ={'functionDataset':{
                                                            'rasterFunction':"GS_Composite_wofs_ss.rft.xml",
                                                            'rasterFunctionArguments':{
                                                                                        'Raster1': WSS01,
                                                                                        'Raster1_info':{'pixelType':6,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY},

                                                                                        'Raster2': WSS02,
                                                                                        'Raster2_info':{'pixelType':6,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY} ,

                                                                                        'Raster3': WSS03,
                                                                                        'Raster3_info':{'pixelType':10,'nCols':nCols,'nRows':nRows,'nBands':noBands,'spatialReference':srsEPSG,
                                                                                                        'XMin':minX,'YMin':minY,'XMax':maxX,'YMax':maxY}
                                                                                       }
                                                            }
                                        }

                    metadata['bandProperties'] = bandProperties
                    try:
                        builtItem['footprint']= footprint_geometry
                    except :
                        arcpy.AddMessage("No valid geometry to update the footprint")
                    builtItem['spatialReference'] = srsEPSG

                elif (tag=='wofs'):
                    bandProperties = [{'bandName':'water'}]

                    rasterInfo={}
                    rasterInfo['pixelType']= pixelType
                    rasterInfo['nCols']= nCols
                    rasterInfo['nRows']= nRows
                    rasterInfo['nBands']= noBands
                    rasterInfo['spatialReference']= srsEPSG
                    rasterInfo['XMin']= minX
                    rasterInfo['YMin']= minY
                    rasterInfo['XMax']= maxX
                    rasterInfo['YMax']= maxY

                    builtItem['raster'] = {'uri': W01 , 'rasterInfo': rasterInfo}
                    builtItem['spatialReference'] = srsEPSG
                    try:
                        builtItem['footprint']= footprint_geometry
                    except :
                        arcpy.AddMessage("No valid geometry to update the footprint")
                    metadata['bandProperties'] = bandProperties
                    try:
                        itemURI['displayName'] = fileName
                    except:
                        ""


                variables={}

                builtItem['variables'] = variables
                builtItem['itemUri'] = itemURI
                builtItem['keyProperties'] = metadata
                builtItemsList = list()
                builtItemsList.append(builtItem)
                return builtItemsList

            except Exception as e:
                print(str(e))
                return None


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# GeoscienceCrawler Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class GeoscienceCrawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()
        self.paths = crawlerProperties['paths']
        self.recurse = crawlerProperties['recurse']
        self.filter = crawlerProperties['filter']
        if ("RasterProxy" in self.filter):
            self.rpFlag = 1
        else:
            self.rpFlag = 0
        if not self.filter:
            self.filter = '*.yaml'

        try:
            self.pathGenerator = self.createGenerator()

        except StopIteration:
            return None

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
                        filename= filename.rstrip()
                        if (filename.startswith("http")):   #if the csv list contains a list of s3 urls
                            yield filename
                        elif (filename.endswith(".yaml") and os.path.exists(filename)):
                            yield filename
            elif (path.endswith(".yaml")):
                yield path


    def __iter__ (self):
        return self

    def next(self):
        ## Return URI dictionary to Builder
        return self.getNextUri()

    def getNextUri(self):

        try:
            self.curPath = next(self.pathGenerator)
            productName = self.utils.getProductName(self.curPath)
            curTag = self.utils.getTag(productName)

            #If the tag or productName was not found in the metadata file or\
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
                'productName': productName,
                'uriProperties':{ 'rpflag':self.rpFlag}
              }

        return uri
