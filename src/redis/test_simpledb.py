import unittest
import sys

import gevent

import simpledb

class TestRedis(unittest.TestCase):

    def setUp(self):
        print("start setup")
        self.client = simpledb.Client(host="127.0.0.1", port=31337)
        print("setup: client setup")
        self.client.flush()
        print("setup: finish")
    
    def test_setting_getting(self):
        """Testing setting and getting values"""
        self.assertEqual(self.client.set("k1", 1), 1)
        self.assertEqual(self.client.set("k2", "hello"), 1)
        self.assertEqual(self.client.set("k3", [1, 2]), 1)
        self.assertEqual(self.client.set("k4", {"sk1": 1, "sk2": 2}), 1)

        # GETTING
        self.assertEqual(self.client.get("k1"), 1)
        self.assertEqual(self.client.get("k2"), "hello")
        self.assertEqual(self.client.get("k3"), [1, 2])
        self.assertEqual(self.client.get("k4"), {"sk1": 1, "sk2": 2})


if __name__ == "__main__":
    server = simpledb.Server(address="127.0.0.1",port=31337)
    t = gevent.spawn(server.run)
    gevent.sleep()
    print("start unittest")
    unittest.main()
    print("end unittest")
