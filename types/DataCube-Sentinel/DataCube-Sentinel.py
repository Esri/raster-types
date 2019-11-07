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
# Name: DataCube-Sentinel
# Description: DataCube-Sentinel python raster type.
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
                    'rasterTypeName': 'DataCube-Sentinel',
                    'builderName': 'SentinelDataCubeBuilder',
                    'description': ("Supports reading of Sentinel1 DataCube data"),
                    'enableClipToFootprint': True,
                    'isRasterProduct': False,
                    'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                    'dataSourceFilter': '*.yaml',
                    'crawlerName': 'SentinelDataCubeCrawler',
                    'productDefinitionName': 'DataCube-Sentinel',
                    'supportedUriFilters': [
                                            {
                                                'name': 'Level2',
                                                'allowedProducts': [
                                                                    'gamma_0',
                                                                   ],
                                                'supportsOrthorectification': True,
                                                'enableClipToFootprint': True,
                                                'supportedTemplates': [
                                                                       'DataCube_S1_SAR',
                                                                      ]
                                            }

                                           ],
                    'processingTemplates': [
                                            {
                                                'name': 'DataCube_S1_SAR',
                                                'enabled': True,
                                                'outputDatasetTag': 'DataCube_S1_SAR',
                                                'primaryInputDatasetTag': 'DataCube_S1_SAR',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'DataCube_S1_SAR.rft.xml'
                                            }
                                           ],
                    #GET THE CORRECT BAND INDEX , MIN MAX WAVELENGTH
                    'bandProperties': [
                                        {
                                            'bandName': 'vh',
                                            'bandIndex': 1,
                                            'wavelengthMin': 180000000.0,  #C band with central frequency of 5.405 GHz
                                            'wavelengthMax': 180000000.0,
                                            'datasetTag': 'SAR'
                                        },
                                        {
                                            'bandName': 'vv',
                                            'bandIndex': 2,
                                            'wavelengthMin': 180000000.0,     #C band with central frequency of 5.405 GHz
                                            'wavelengthMax': 180000000.0,
                                            'datasetTag': 'SAR'
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
            page = client.get_object(Bucket=bucket,Key=path,RequestPayer='requester')
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
# SentinelDataCube builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##

class SentinelDataCubeBuilder():

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
                '    <Size c="1" x="5529" y="5529"/>\n'  \
                '    <PageSize c="1" x="512" y="512"/>\n'  \
                '    <Compression>LERC</Compression>\n'  \
                '    <DataType>Float32</DataType>\n'  \
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
            NRT01=NRT02=""
            yamldir = os.path.dirname(_yamlpath)

            if (_yamlpath.startswith("http:")):
                doc = self.utils.readYamlS3(_yamlpath)
                if (doc is None or 'image' not in doc or 'bands' not in doc['image']):
##                    print  ('Err. Invalid input format!')
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

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['vh']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['vv']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)


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

                NRT01 = self.embedMRF(inputDir,(doc['image']['bands']['vh']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)
                NRT02 = self.embedMRF(inputDir,(doc['image']['bands']['vv']['path']),maxX,maxY,minX,minY,prjString,protocol,cachepath)

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
                NRT01 = os.path.join(yamldir,(doc['image']['bands']['vh']['path']))
                NRT02 = os.path.join(yamldir,(doc['image']['bands']['vv']['path']))

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
            if (itemURI['tag'] == "DataCube_S1_SAR"):
                #NBART
                bandProperties = [{'bandName':'vh'},
                                    {'bandName':'vv'}]

                builtItem['raster'] ={'functionDataset':{
                                                        'rasterFunction':"DataCube_SAR_Composite.rft.xml",
                                                        'rasterFunctionArguments':{
                                                                                    'Raster1': NRT01,
                                                                                    'Raster1_rasterInfo':{'pixelType':10,'ncols':5535,'nRows':5535,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
                                                                                    'Raster2': NRT02,
                                                                                    'Raster2_rasterInfo':{'pixelType':10,'ncols':5535,'nRows':5535,'nBands':1,'spatialReference':srsWKT,'xMin':minX,'yMin':minY,'xMax':maxX,'yMax':maxY},
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
# SentinelDataCube Crawlerclass
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
class SentinelDataCubeCrawler():

    def __init__(self, **crawlerProperties):
        self.utils = Utilities()
        try:
            self.paths = crawlerProperties['paths']
            self.recurse = crawlerProperties['recurse']
            self.filter = crawlerProperties['filter']
            self.run = 1
        except:
##            print ('Error in crawler properties')
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
        for tag in ["DataCube_S1_SAR"]:      #Sentinel1
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