__author__ = 'aalkhatib'
import unittest
from mongoctl.mongoctl import version_obj, is_valid_version

class VersionFunctionsTest(unittest.TestCase):
    def test_version_functions(self):
        self.assertTrue(version_obj("1.2.0") < version_obj("1.2.1"))
        self.assertFalse(version_obj("1.2.0") < version_obj("1.2.0"))
        self.assertFalse(version_obj("1.2.0") < version_obj("1.2.0C"))
        self.assertFalse(version_obj("1.2.0-rc1") < version_obj("1.2.0-rc0"))
        self.assertTrue(version_obj("1.2.0-rc0") < version_obj("1.2.0-rc1"))
        self.assertTrue(version_obj("1.2.0-rc0") < version_obj("1.2.0"))
        self.assertTrue(version_obj("1.2.0-rc0") < version_obj("1.2.1"))

        self.assertTrue(is_valid_version("1.0.1"))
        self.assertTrue(is_valid_version("0.1"))
        self.assertFalse(is_valid_version("1"))
        self.assertTrue(is_valid_version("0.1a"))
        self.assertTrue(is_valid_version("0.1c"))
        self.assertTrue(is_valid_version("1.8.9-rc0"))
        self.assertFalse(is_valid_version("a.1.2.3.4"))



