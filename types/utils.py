#------------------------------------------------------------------------------
# Copyright 2016 Esri
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

__all__ = ['Trace',
           'DataSourceType',]

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- #

class Trace():
    def __init__(self):
        ctypes = __import__('ctypes')
        self.trace = ctypes.windll.kernel32.OutputDebugStringA
        self.trace.argtypes = [ctypes.c_char_p]
        self.c_char_p = ctypes.c_char_p

    def log(self, s):
        self.trace(self.c_char_p(s.encode('utf-8')))
        return s

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- #

class DataSourceType():
    File = 1
    Folder = 2
    RasterDataset = 128

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- #