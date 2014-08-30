# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import unittest
import dio
import dio.errors

import settings


@dio.processor
@dio.restart_on_error
def fail(out=None, err=None):
	d = yield
	raise ValueError("an example bad value situation")


class ErrorsTestCase(unittest.TestCase):
	def setUp(self):
		"""Send out/err to inspectable accumulators rather than the screen."""
		self.out = []
		self.err = []

		dio.default_out = dio.buffer_out(out=self.out)
		dio.default_err = dio.buffer_out(out=self.err)

	def test_error_lazydict(self):
		try:
			1 + "1"
		except TypeError, e:
			d = dio.errors.e2d(e)
			self.assertEqual(
				d['error'],
				"TypeError: unsupported operand type(s) for +: 'int' and 'str'",
			)
			self.assertEqual(
				d['exception_type'],
				"TypeError",
			)
			self.assertEqual(
				d['exception_message'],
				"unsupported operand type(s) for +: 'int' and 'str'",
			)
	
	def test_error_in_pipeline(self):
		#--- run it

		dio.source([{}, {}],
			out=fail()
		)
		

		#--- inspect output

		self.assertEqual(
			len(self.out),
			0,
		)

		self.assertEqual(
			len(self.err),
			2,
		)
		for d in self.err:
			self.assertTrue(d.has_key('error'))


if __name__=='__main__':
	unittest.main()
