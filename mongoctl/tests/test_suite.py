import unittest

from version_functions_test import VersionFunctionsTest
from basic_test import BasicMongoctlTest
from master_slave_test import MasterSlaveTest
from replicaset_test import ReplicasetTest
from misc_test import MiscTest
from auth_replicaset_test import AuthReplicasetTest
###############################################################################
all_suites = [
    unittest.TestLoader().loadTestsFromTestCase(VersionFunctionsTest),
    unittest.TestLoader().loadTestsFromTestCase(BasicMongoctlTest),
    unittest.TestLoader().loadTestsFromTestCase(MasterSlaveTest),
    unittest.TestLoader().loadTestsFromTestCase(ReplicasetTest),
    unittest.TestLoader().loadTestsFromTestCase(AuthReplicasetTest),
    unittest.TestLoader().loadTestsFromTestCase(MiscTest)
]
###############################################################################
# booty
###############################################################################
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(all_suites))


