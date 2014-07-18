# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import sys, time, string, unittest
import mock

import dio.processor
from dio.processor import processor, source, filter, apply, uniq


class ProcessorTestCase(unittest.TestCase):
	def setUp(self):
		dio.processor.default_out = dio.processor.out_accumulator()
		dio.processor.default_err = dio.processor.err_accumulator()

		dio.processor.accumulated_out = []
		dio.processor.accumulated_err = []

	def test_pipeline_out_err(self):
		@processor
		def filter1(out=None, err=None):
			while True:
				d = yield
				if d['letter'] in 'xyz':
					out.send(d)
				if d['letter'] in 'e':
					err.send({'error':'test error message for e'})

		@processor
		def filter2(out=None, err=None):
			while True:
				d = yield
				if d['letter'] in 'xy':
					out.send(d)
				if d['letter'] in 'z':
					err.send({'error':'test error message for z'})

		@processor
		def verifier(out=None, err=None):
			while True:
				d = yield
				assert d['letter'] in 'xy', "the filter did not produce the expected results"
				out.send(d)

		import string
		source([ {'letter':c} for c in string.ascii_letters ],
			out=filter1(
				out=filter2(
					out=verifier()
				)
			)
		)

		self.assertEqual(
			dio.processor.accumulated_out,
			[
				{'letter': 'x'},
				{'letter': 'y'},
			],
			"unexpected out"
		)
		self.assertEqual(
			dio.processor.accumulated_err,
			[
				{'error': 'test error message for e'},
				{'error': 'test error message for z'},
			],
			"unexpected err"
		)

	def test_apply(self):
		def random_gate(d):
			import random
			if random.randint(0,1)==0:
				yield d

		source([ {'letter':c} for c in string.ascii_letters ],
			out=apply(random_gate)
		)

		self.assertTrue(len(dio.processor.accumulated_out) > 0)
		self.assertTrue(len(dio.processor.accumulated_out) < len(string.ascii_letters)) #(there is an astronomically small chance this will randomly not be true)
		self.assertEqual(len(dio.processor.accumulated_err), 0)

	def test_uniq(self):
		source(({1:'foo'}, {1:'foo'}, {1:'bar'}),
			out=uniq()
		)

		l_want = 2
		l_got = len(dio.processor.accumulated_out)
		self.assertEqual(l_want, l_got,
			"uniq did not yield the proper number of output dicts; expected %d, got %s" % (l_want, l_got)
		)


if __name__=='__main__':
	unittest.main()
