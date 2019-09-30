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
# Name: DataCube-Landsat
# Description: DataCube-Landsat python raster type.
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
    print ('Err. {}'.format(str(e)))
    exit(1)

class DataSourceType():
    File = 1
    Folder = 2

class RasterTypeFactory():

    def getRasterTypesInfo(self):
        self.dacq_auxField = arcpy.Field()              #center_dt
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
                    'rasterTypeName': 'DataCube-Landsat',
                    'builderName': 'LandsatDataCubeBuilder',
                    'description': ("Supports reading of Landsat8/Landsat7 DataCube data"),
                    'enableClipToFootprint': True,
                    'isRasterProduct': False,
                    'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                    'dataSourceFilter': '*.yaml',
                    'crawlerName': 'LandsatDataCubeCrawler',
                    'productDefinitionName': 'DataCube-Landsat',
                    'supportedUriFilters': [
                                            {
                                                'name': 'Level2',
                                                'allowedProducts': [
                                                                    'LaSRC',
                                                                   ],
                                                'supportsOrthorectification': True,
                                                'enableClipToFootprint': True,
                                                'supportedTemplates': [
                                                                       'DataCube_L8_MS_QA',
                                                                       'DataCube_L8_MS',
                                                                       'DataCube_L7_MS',
                                                                       'DataCube_L7_MS_QA'
                                                                      ]
                                            }

                                           ],
                    'processingTemplates': [
                                            {
                                                'name': 'DataCube_L8_MS_QA',
                                                'enabled': True,
                                                'outputDatasetTag': 'DataCube_L8_MS_QA',
                                                'primaryInputDatasetTag': 'DataCube_L8_MS_QA',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'DataCube_L8_MS_QA.rft.xml'
                                            },

                                            {
                                                'name': 'DataCube_L8_MS',
                                                'enabled': True,
                                                'outputDatasetTag': 'DataCube_L8_MS',
                                                'primaryInputDatasetTag': 'DataCube_L8_MS',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'DataCube_L8_MS.rft.xml'
                                            },

                                            {
                                                'name': 'DataCube_L7_MS_QA',
                                                'enabled': True,
                                                'outputDatasetTag': 'DataCube_L7_MS_QA',
                                                'primaryInputDatasetTag': 'DataCube_L7_MS_QA',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'DataCube_L7_MS_QA.rft.xml'
                                            },

                                            {
                                                'name': 'DataCube_L7_MS',
                                                'enabled': True,
                                                'outputDatasetTag': 'DataCube_L7_MS',
                                                'primaryInputDatasetTag': 'DataCube_L7_MS',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'DataCube_L7_MS.rft.xml'
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
                                            'bandName': 'aerosol_qa',
                                            'bandIndex': 7,
                                            'wavelengthMin': 0.0,
                                            'wavelengthMax': 255.0,
                                            'datasetTag': 'QA'
                                        },
                                        {
                                            'bandName': 'coastal_aerosol',
                                            'bandIndex': 8,
                                            'wavelengthMin': 435.0,
                                            'wavelengthMax': 451.0,
                                            'datasetTag': 'QA'
                                        },
                                        {
                                            'bandName': 'pixel_qa',
                                            'bandIndex': 9,
                                            'wavelengthMin': 0.0,
                                            'wavelengthMax': 255.0,
                                            'datasetTag': 'QA'
                                        },
                                        {
                                            'bandName': 'radsat_qa',
                                            'bandIndex': 10,
                                            'wavelengthMin': 0.0,
                                            'wavelengthMax': 255.0,
                                            'datasetTag': 'QA'
                                        },
                                        {
                                            'bandName': 'atmos_opacity',
                                            'bandIndex': 11,
                                            'wavelengthMin': 0.0,
                                            'wavelengthMax': 255.0,
                                            'datasetTag': 'QA'
                                        },
                                        {
                                            'bandName': 'cloud_qa',
                                            'bandIndex': 12,
                                            'wavelengthMin': 0.0,
                                            'wavelengthMax': 255.0,
                                            'datasetTag': 'QA'
                                        }
                                      ],
                    #GET THE CORRECT BAND INDEX , MIN MAX WAVELENGTH

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

    def readYamlS3(self,path):          #to read the yaml file located on S3
        page=requests.get(path,stream=True,timeout=None)
        try:
            doc= (yaml.load(page.content))
        except yaml.YAMLError as exc:
            print(exc)
            return None
        return doc

    def readYamlS3_boto3(self,bucket,path):          #to read the yaml file located on S3
        client = boto3.client('s3')
        try:
            page = client.get_object(Bucket=bucket,Key=path,RequestPayer='requester')
            doc = (yaml.load(page['Body'].read()))
        except yaml.YAMLError as exc:
            print(exc)
            return None
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
# LandsatDataCube builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class LandsatDataCubeBuilder():

    def __init__(self, **kwargs):
        self.utils = Utilities()

    def canOpen(self, datasetPath):
        return True

    def embedMRF(self,inputDir,fileName,maxX,maxY,minX,minY,prjString,protocol,cachepath):

        try:
            #create template
            cachingMRF = \
                '<MRF_META>\n'  \
                '  <CachedSource>\n'  \
                '    <Source>/{8}{0}/{1}</Source>\n'  \
                '  </CachedSource>\n'  \
                '  <Raster>\n'  \
                '    <Size c="1" x="3500" y="3500"/>\n'  \
                '    <PageSize c="1" x="512" y="512"/>\n'  \
                '    <Compression>LERC</Compression>\n'  \
                '    <DataType>Int16</DataType>\n'  \
                '   <DataValues NoData="-9999" />\n' \
                '  <DataFile>z:/mrfcache/{9}/{7}.mrf_cache</DataFile><IndexFile>z:/mrfcache/{9}/{7}.mrf_cache</IndexFile></Raster>\n'  \
                '  <Rsets model="uniform" scale="2"/>\n'  \
                '  <GeoTags>\n'  \
                '    <BoundingBox maxx="{2}" maxy="{3}" minx="{4}" miny="{5}"/>\n'  \
                '    <Projection>{6}</Projection>\n'  \
                '  </GeoTags>\n'  \
                '  <Options>V2=ON</Options>\n'  \
                '</MRF_META>\n'.format(inputDir,fileName,maxX,maxY,minX,minY,prjString,fileName[0:-4],protocol,cachepath)

        except Exception as exp:
            log.Message(str(exp),log.const_critical_text)

        return cachingMRF


    def build(self, itemURI):
     # Make sure that the itemURI dictionary contains items
        if (len(itemURI) <= 0):
            return None
        try:
            # ItemURI dictionary passed from craxwler containing
            # path, tag, display name, group name, product type
            path=None
            if ('path' in itemURI):
                _yamlpath = itemURI['path']
            else:
                return None

            #for each band in the image generate the path
            NRT01=NRT02=NRT03=NRT04=NRT05=NRT06=NRT07=NRT08=NRT09=NRT10=""
            yamldir = os.path.dirname(_yamlpath)

            if (_yamlpath.startswith("http:")):
                doc = self.utils.readYamlS3(_yamlpath)
                if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
                    print  ('Err. Invalid input format!')
                    return False

                lastIdx= _yamlpath.rfind('/')
##                startIdx= _yamlpath.find('.com')+5  #plus 5 to get the index of the character after .com/
                inputDir= _yamlpath[7:lastIdx]  #along with the bucket name

                refPoints= doc['grid_spatial']['projection']['geo_ref_points']
                maxX= refPoints['lr']['x']
                maxY= refPoints['ur']['y']
                minX= refPoints['ll']['x']
                minY= refPoints['ll']['y']

                spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                spatialIdx= spatialRef.find(':')
                spatialId= int(spatialRef[spatialIdx+1:])
                prjString= arcpy.SpatialReference(spatialId).exportToString()
                protocol ='vsicurl/http://'
                cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")].replace(".s3.amazonaws.com","")

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['green']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT03 = self.embedMRF(inputDir,(doc['image']['bands']['red']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT04 = self.embedMRF(inputDir,(doc['image']['bands']['nir']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT05 = self.embedMRF(inputDir,(doc['image']['bands']['swir1']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT06 = self.embedMRF(inputDir,(doc['image']['bands']['swir2']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                try:
                    NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['aerosol_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS8
                except:
                    NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['atmos_opacity']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS7
                try:
                    NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['coastal_aerosol']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS8
                except:
                    NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['cloud_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)          #LS7
                NRT09 = self.embedMRF(inputDir,(doc['image']['bands']['pixel_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT10 = self.embedMRF(inputDir,(doc['image']['bands']['radsat_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
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

                spatialRef= doc['grid_spatial']['projection']['spatial_reference']
                spatialIdx= spatialRef.find(':')
                spatialId= int(spatialRef[spatialIdx+1:])
                prjString= arcpy.SpatialReference(spatialId).exportToString()
                protocol ='vsis3/'
                cachepath = _yamlpath.split("//")[1][0:_yamlpath.split("//")[1].rfind("/")]

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['blue']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['green']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT03 = self.embedMRF(inputDir,(doc['image']['bands']['red']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT04 = self.embedMRF(inputDir,(doc['image']['bands']['nir']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT05 = self.embedMRF(inputDir,(doc['image']['bands']['swir1']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT06 = self.embedMRF(inputDir,(doc['image']['bands']['swir2']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                try:
                    NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['aerosol_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS8
                except:
                    NRT07 = self.embedMRF(inputDir,(doc['image']['bands']['atmos_opacity']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS7
                try:
                    NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['coastal_aerosol']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath) #LS8
                except:
                    NRT08 = self.embedMRF(inputDir,(doc['image']['bands']['cloud_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)          #LS7
                NRT09 = self.embedMRF(inputDir,(doc['image']['bands']['pixel_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT10 = self.embedMRF(inputDir,(doc['image']['bands']['radsat_qa']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
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

                NRT01 = os.path.join(yamldir,(doc['image']['bands']['blue']['path']))
                NRT02 = os.path.join(yamldir,(doc['image']['bands']['green']['path']))
                NRT03 = os.path.join(yamldir,(doc['image']['bands']['red']['path']))
                NRT04 = os.path.join(yamldir,(doc['image']['bands']['nir']['path']))
                NRT05 = os.path.join(yamldir,(doc['image']['bands']['swir1']['path']))
                NRT06 = os.path.join(yamldir,(doc['image']['bands']['swir2']['path']))
                try:
                    NRT07 = os.path.join(yamldir,(doc['image']['bands']['aerosol_qa']['path'])) #LS8
                except:
                    NRT07 = os.path.join(yamldir,(doc['image']['bands']['atmos_opacity']['path'])) #LS7
                try:
                    NRT08 = os.path.join(yamldir,(doc['image']['bands']['coastal_aerosol']['path'])) #LS8
                except:
                    NRT08 = os.path.join(yamldir,(doc['image']['bands']['cloud_qa']['path'])) #LS7
                NRT09 = os.path.join(yamldir,(doc['image']['bands']['pixel_qa']['path']))
                NRT10 = os.path.join(yamldir,(doc['image']['bands']['radsat_qa']['path']))


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

            ## Check for URI.
################# ESPG CODE
            srsWKT = 0
            projectionNode = doc['grid_spatial']['projection']['spatial_reference']
            if (projectionNode is not None):
                srsWKT =int(projectionNode.split(":")[1])

################# DEFINE A DICTIONARY OF VARIABLES
            variables = {}

################# #Depending upon the tag name in the itemURI pass the appropriate bandProperties dictionary and RFT
            builtItem = {}
            if (itemURI['tag'] == "DataCube_L8_MS" or itemURI['tag'] == "DataCube_L7_MS"):
                #NBART
                bandProperties = [{'bandName':'blue'},
                                    {'bandName':'green'},
                                    {'bandName':'red'},
                                    {'bandName':'nir'},
                                    {'bandName':'swir1'},
                                    {'bandName':'swir2'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"DataCube_MS_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NRT01,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NRT02,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NRT03,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NRT04,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': NRT05,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': NRT06,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

            elif (itemURI['tag'] == "DataCube_L8_MS_QA"):
                bandProperties = [{'bandName':'blue'},
                                    {'bandName':'green'},
                                    {'bandName':'red'},
                                    {'bandName':'nir'},
                                    {'bandName':'swir1'},
                                    {'bandName':'swir2'},
                                    {'bandName':'aerosol_qa'},
                                    {'bandName':'coastal_aerosol'},
                                    {'bandName':'pixel_qa'},
                                    {'bandName':'radsat_qa'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"DataCube_MS_QA_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NRT01,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NRT02,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NRT03,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NRT04,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': NRT05,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': NRT06,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': NRT07,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': NRT08,
                                                                                    'Raster8_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': NRT09,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': NRT10,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

            elif (itemURI['tag'] == "DataCube_L7_MS_QA"):
                bandProperties = [{'bandName':'blue'},
                    {'bandName':'green'},
                    {'bandName':'red'},
                    {'bandName':'nir'},
                    {'bandName':'swir1'},
                    {'bandName':'swir2'},
                    {'bandName':'atmos_opacity'},
                    {'bandName':'cloud_qa'},
                    {'bandName':'pixel_qa'},
                    {'bandName':'radsat_qa'}]
                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"DataCube_MS_QA_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NRT01,
                                                                                    'Raster1_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NRT02,
                                                                                    'Raster2_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster3': NRT03,
                                                                                    'Raster3_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster4': NRT04,
                                                                                    'Raster4_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster5': NRT05,
                                                                                    'Raster5_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster6': NRT06,
                                                                                    'Raster6_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster7': NRT07,
                                                                                    'Raster7_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster8': NRT08,
                                                                                    'Raster8_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster9': NRT09,
                                                                                    'Raster9_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster10': NRT10,
                                                                                    'Raster10_rasterInfo':{'pixelType':6,'ncols':3500,'nRows':3500,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY}
                                                                                   }
                                                        }
                                    }

################### Assemble everything into an outgoing dictionary
            cordsList = doc['grid_spatial']['projection']['valid_data']['coordinates']
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
            print(str(e))
        return None

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# LandsatDataCube Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
class LandsatDataCubeCrawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()
        try:
            self.paths = crawlerProperties['paths']
            self.recurse = crawlerProperties['recurse']
            self.filter = crawlerProperties['filter']
            self.run = 1
        except:
            print ('Error in crawler properties')
            return None
        if (self.filter is (None or "")):
            self.filter = '*.yaml'
        try:
            self.pathGenerator = self.createGenerator()
        except StopIteration:
            return None

        try:
            self.tagGenerator = self.createTagGenerator()   #reinitialize tag generator
        except StopIteration:
            return None

    def createTagGenerator(self):
        for tag in ["DataCube_L8_MS","DataCube_L8_MS_QA","DataCube_L7_MS","DataCube_L7_MS_QA"]:      #Landsat8 and Landsat7
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
            if (self.run ==1):          #the path generator should kick in first (for the very first record) before the tag generator kicks in otherwise the number of URIs generated will be one less than the number of tags.
                try:
                    self.curPath = next(self.pathGenerator)
                    self.run=10
                except:
                    return None
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
        except StopIteration:
            return None
        uri = {
                'path': self.curPath,
                'displayName': os.path.basename(self.curPath).partition(".")[0],
                'tag': curTag,
                'groupName': os.path.basename(self.curPath).partition(".")[0],
##                'productName':productName
              }
        return uri