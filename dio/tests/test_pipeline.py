# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import sys, time, string, cStringIO, unittest
import dio
import dio.coreutils

import settings


class ProcessorTestCase(unittest.TestCase):
	def setUp(self):
		"""Send out/err to inspectable accumulators rather than the screen."""
		self.out = []
		self.err = []

		dio.default_out = dio.buffer_out(out=self.out)
		dio.default_err = dio.buffer_out(out=self.err)


	def test_pipeline_out_err(self):
		"""Test basic pipeline functionality, including out/err."""

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
			self.out,
			[
				{'letter': 'x'},
				{'letter': 'y'},
			],
			"unexpected out"
		)
		self.assertEqual(
			self.err,
			[
				{'error': 'test error message for e'},
				{'error': 'test error message for z'},
			],
			"unexpected err"
		)

	def test_restart_on_error(self):
		"""Test processors the restart upon errors."""

		#--- a processor that fails on every other (even) inputs

		@dio.processor
		@dio.restart_on_error
		def every_other_fails(out=None, err=None):
			i = 0
			while True:
				d = yield
				if i%2 == 1:
					raise Exception("bad one")
				out.send(d)
				i += 1


		#--- run it

		dio.source(({"name":"a"}, {"name":"b"}, {"name":"c"}),
			out=every_other_fails()
		)


		#--- inspect output

		#the first and third should be in out
		l_want = 2
		l_got = len(self.out)
		self.assertEqual(l_want, l_got)

		#the second should've generated an entry in err
		l_want = 1
		l_got = len(self.err)
		self.assertEqual(l_want, l_got)

	def test_restart_on_error_in_pipeline(self):
		"""Make sure a restart-on-error processor does not restart others.

		An example of something that has state is dio.wc.  This tests sends the
		output of a restart-on-error processor (that also produces errors) to
		dio.wc and ensure there is one total count rather than two separate
		partial counts.
		"""

		#--- a processor that fails on every other (even) inputs

		@dio.processor
		@dio.restart_on_error
		def every_other_fails(out=None, err=None):
			i = 0
			while True:
				d = yield
				if i%2 == 1:
					raise Exception("bad one")
				out.send(d)
				i += 1


		#--- run it

		dio.source(({"name":"a"}, {"name":"b"}, {"name":"c"}),
			out=every_other_fails(
				out=dio.coreutils.wc()
			)
		)


		#--- inspect output

		expected_count = 2

		self.assertEqual(len(self.out), 1)
		self.assertEqual(
			self.out[0]["count"],
			expected_count
		)

		#(there is that one in stderr, too, but we tested that above)

	def test_pickling(self):
		"""Test serialization by pickling."""

		#--- run it

		#set stdout to be a string we can feed back in
		fout = cStringIO.StringIO()

		#run the serialization
		dio.source(({'foo':'bar'}, {'x':42}),
			out=dio.pickle_out(out=fout)
		)

		#set stdin to be the string we wrote
		fin = cStringIO.StringIO(fout.getvalue())

		#run the deserialization
		dio.pickle_in(inn=fin)


		#--- inspect output

		self.assertEqual(
			self.out,
			[
				{'foo': 'bar'},
				{'x': 42},
			],
		)
		self.assertEqual(
			self.err,
			[],
		)

	def test_json(self):
		"""Test serialization by json."""

		#--- run it

		#set stdout to be a string we can feed back in
		fout = cStringIO.StringIO()

		#run the serialization
		dio.source(({'foo':'bar'}, {'x':42}),
			out=dio.json_out(out=fout)
		)

		#set stdin to be the string we wrote
		fin = cStringIO.StringIO(fout.getvalue())

		#run the deserialization
		dio.json_in(inn=fin)


		#--- inspect output

		self.assertEqual(
			self.out,
			[
				{'foo': 'bar'},
				{'x': 42},
			],
		)
		self.assertEqual(
			self.err,
			[],
		)

	def test_apply(self):
		"""Test dio.apply."""

		#--- something to apply

		def even_letters(d):
			if ord(d['letter'])%2 == 0:
				yield d


		#--- run it

		dio.source([ {'letter':c} for c in string.ascii_lowercase ],
			out=dio.apply(even_letters)
		)


		#--- inspect output

		self.assertEqual(len(self.out), 13)
		self.assertEqual(len(self.err), 0)

	def test_tidy(self):
		"""Test dio.tidy."""

		#a couple dicts with keys for each letter in the alphabet
		inn = [
			dict([ (c,"foo") for c in string.ascii_lowercase ]),
			dict([ (c,"foo") for c in string.ascii_lowercase ]),
		]

		#want to tidy to only these keys:
		keys = ('a', 'b', 'c')

		#--- run it
		dio.source(inn, out=dio.tidy(keys))


		#--- inspect output

		self.assertEqual(len(self.out), 2)  #two in, two out
		for d in self.out:
			self.assertTrue(len(d.keys()), 3)  #only three keys total
			for k in keys:
				self.assertTrue(k in d)  #and specifically those keys

	def test_uniq(self):
		"""Test dio.uniq."""

		#--- run it

		dio.source(({"name":"foo"}, {"name":"foo"}, {"name":"bar"}),
			out=dio.coreutils.uniq()
		)


		#--- inspect output

		l_want = 2
		l_got = len(self.out)
		self.assertEqual(l_want, l_got,
			"uniq did not yield the proper number of output dicts; expected %d, got %s" % (l_want, l_got)
		)

	def test_wc(self):
		"""Test dio.wc."""

		#--- run it

		dio.source(({"name":"foo"}, {"name":"foo"}, {"name":"bar"}),
			out=dio.coreutils.wc()
		)


		#--- inspect output

		expected_count = 3

		self.assertEqual(len(self.out), 1)
		self.assertEqual(
			self.out[0]["count"],
			expected_count
		)


if __name__=='__main__':
	unittest.main()
