# The MIT License

# Copyright (c) 2012 ObjectLabs Corporation

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
__author__ = 'aalkhatib'
import unittest
from mongoctl.mongodb_version import make_version_info, is_valid_version

class VersionFunctionsTest(unittest.TestCase):
    def test_version_functions(self):
        self.assertTrue(make_version_info("1.2.0") < make_version_info("1.2.1"))
        self.assertFalse(make_version_info("1.2.0") < make_version_info("1.2.0"))
        self.assertFalse(make_version_info("1.2.0") < make_version_info("1.2.0C"))
        self.assertFalse(make_version_info("1.2.0-rc1") < make_version_info("1.2.0-rc0"))
        self.assertTrue(make_version_info("1.2.0-rc0") < make_version_info("1.2.0-rc1"))
        self.assertTrue(make_version_info("1.2.0-rc0") < make_version_info("1.2.0"))
        self.assertTrue(make_version_info("1.2.0-rc0") < make_version_info("1.2.1"))

        self.assertTrue(is_valid_version("1.0.1"))
        self.assertTrue(is_valid_version("0.1"))
        self.assertFalse(is_valid_version("1"))
        self.assertTrue(is_valid_version("0.1a"))
        self.assertTrue(is_valid_version("0.1c"))
        self.assertTrue(is_valid_version("1.8.9-rc0"))
        self.assertFalse(is_valid_version("a.1.2.3.4"))



