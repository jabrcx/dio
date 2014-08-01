# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import sys, time, string, cStringIO, unittest

import dio
from dio import processor


class CliTestCase(unittest.TestCase):
	def setUp(self):
		dio.default_out = dio.out_accumulator()
		dio.default_err = dio.err_accumulator()

		dio.accumulated_out = []
		dio.accumulated_err = []

	def test_pipeline_out_err(self):
		pass


if __name__=='__main__':
	unittest.main()
