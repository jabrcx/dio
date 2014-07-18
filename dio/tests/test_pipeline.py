# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import sys, time, string, cStringIO, unittest

import dio
from dio import processor, source, out_pickle, in_pickle, filter, apply, uniq


class ProcessorTestCase(unittest.TestCase):
	def setUp(self):
		dio.default_out = dio.out_accumulator()
		dio.default_err = dio.err_accumulator()

		dio.accumulated_out = []
		dio.accumulated_err = []

	def test_pipeline_out_err(self):
		#--- some processors

		@dio.processor
		def filter1(out=None, err=None):
			while True:
				d = yield
				if d['letter'] in 'xyz':
					out.send(d)
				if d['letter'] in 'e':
					err.send({'error':'test error message for e'})

		@dio.processor
		def filter2(out=None, err=None):
			while True:
				d = yield
				if d['letter'] in 'xy':
					out.send(d)
				if d['letter'] in 'z':
					err.send({'error':'test error message for z'})

		@dio.processor
		def verifier(out=None, err=None):
			while True:
				d = yield
				assert d['letter'] in 'xy', "the filter did not produce the expected results"
				out.send(d)


		#--- run it

		dio.source([ {'letter':c} for c in string.ascii_letters ],
			out=filter1(
				out=filter2(
					out=verifier()
				)
			)
		)


		#--- inspect output

		self.assertEqual(
			dio.accumulated_out,
			[
				{'letter': 'x'},
				{'letter': 'y'},
			],
			"unexpected out"
		)
		self.assertEqual(
			dio.accumulated_err,
			[
				{'error': 'test error message for e'},
				{'error': 'test error message for z'},
			],
			"unexpected err"
		)

	def test_apply(self):
		#--- something to apply

		def random_gate(d):
			import random
			if random.randint(0,1)==0:
				yield d


		#--- run it

		dio.source([ {'letter':c} for c in string.ascii_letters ],
			out=dio.apply(random_gate)
		)


		#--- inspect output

		self.assertTrue(len(dio.accumulated_out) > 0)
		self.assertTrue(len(dio.accumulated_out) < len(string.ascii_letters)) #(there is an astronomically small chance this will randomly not be true)
		self.assertEqual(len(dio.accumulated_err), 0)

	def test_uniq(self):
		#--- run it

		dio.source(({1:'foo'}, {1:'foo'}, {1:'bar'}),
			out=dio.uniq()
		)


		#--- inspect output

		l_want = 2
		l_got = len(dio.accumulated_out)
		self.assertEqual(l_want, l_got,
			"uniq did not yield the proper number of output dicts; expected %d, got %s" % (l_want, l_got)
		)
	
	def test_pickling(self):
		#stash original stdio streams
		stdin  = sys.stdin
		stdout = sys.stdout
		
		try:
			#--- run it

			#set stdout to be a string we can feed back in
			sys.stdout = cStringIO.StringIO()
			
			#run the serialization
			dio.source(({'foo':'bar'}, {'x':42}),
				out=dio.out_pickle()
			)

			#set stdin to be the string we wrote
			sys.stdin = cStringIO.StringIO(sys.stdout.getvalue())

			#run the deserialization
			dio.in_pickle()


			#--- inspect output

			self.assertEqual(
				dio.accumulated_out,
				[
					{'foo': 'bar'},
					{'x': 42},
				],
			)
			self.assertEqual(
				dio.accumulated_err,
				[],
			)

		finally:
			#reset original stdio streams
			sys.stdin  = stdin
			sys.stdout = stdout


if __name__=='__main__':
	unittest.main()
