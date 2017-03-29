#------------------------------------------------------------------------------
# Copyright 2017 Esri
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

import utils

class RasterTypeFactory():

    def getRasterTypesInfo(self):

        return [
                {
                    'rasterTypeName': 'Python Raster Dataset Sample',
                    'builderName': 'PythonRDSBuilder',
                    'description': 'Supports all tif Datasets',
                    'supportsOrthorectification': True,
                    'enableClipToFootprint': False,
                    'allowSimplification': False,
                    'isRasterProduct': False,
                    'dataSourceType': (utils.DataSourceType.File | utils.DataSourceType.Folder | utils.DataSourceType.RasterDataset),
                    'dataSourceFilter': '*.tif',
                    'processingTemplates': [
                                            {
                                                'name': 'Default',
                                                'enabled': True,
                                                'outputDatasetTag': 'Dataset',
                                                'primaryInputDatasetTag': 'Dataset',
                                                'isProductTemplate': False,
                                                'functionTemplate': 'identity.rft.xml'
                                            },
                                            {
                                                'name': 'Stretch',
                                                'enabled': False,
                                                'outputDatasetTag': 'Dataset',
                                                'primaryInputDatasetTag': 'Dataset',
                                                'isProductTemplate': False,
                                                'functionTemplate': 'stretch.rft.xml'
                                            }
                                           ]
                }
               ]


class PythonRDSBuilder():

    def __init__(self, **kwargs):
        self.RasterTypeName = 'Python Raster Dataset Sample'

    def build(self, itemURI):

        if len(itemURI) <= 0:
            return None

        try:
            path = None
            if 'path' in itemURI:
                path = itemURI['path']
            else:
                return None

            builtItem = {}
            builtItem['raster'] = {'uri': path}
            builtItem['itemUri'] = itemURI

            builtItemsList = list()
            builtItemsList.append(builtItem)
            return builtItemsList

        except:
            return None