# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""


import sys, itertools, unittest
import dio

import settings

class MinMaxTestCase(unittest.TestCase):
	def test_min(self):
		"""Test basic usage of max_().

		Send in values {0..9}, repeated twice.  Min three should be two 0s and
		a 1.
		"""

		n = 3
		key = 'x'

		input = \
			[ {key:v} for v in itertools.chain(xrange(10), xrange(10)) ]
		results_expected = \
			[ {key:0}, {key:0}, {key:1} ]  #(order may be different)

		results = []
		dio.source(input,
			out=dio.min_(n, key,
				out=dio.buffer_out(
					out=results
				)
			)
		)

		self.assertEqual(len(results), n)
		self.assertEqual(
			sorted([d[key] for d in results]),
			sorted([d[key] for d in results_expected]),
		)

	def test_max(self):
		"""Test basic usage of max_().

		Send in values {0..9}, repeated twice.  Max three should be two 9s and
		an 8.
		"""

		n = 3
		key = 'x'

		input = \
			[ {key:v} for v in itertools.chain(xrange(10), xrange(10)) ]
		results_expected = \
			[ {key:8}, {key:9}, {key:9} ]  #(order may be different)

		results = []
		dio.source(input,
			out=dio.max_(n, key,
				out=dio.buffer_out(
					out=results
				)
			)
		)

		self.assertEqual(len(results), n)
		self.assertEqual(
			sorted([d[key] for d in results]),
			sorted([d[key] for d in results_expected]),
		)


if __name__=='__main__':
	unittest.main()
