# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""

import time, unittest

import dio
from dio import lazydict

import settings
from eglib import ExampleLazyDict, x_age, x_math


#--- TestCases

class LazyDictTestCase(unittest.TestCase):
	def setUp(self):
		#--- inputs

		self.in_name = 'John Smith'

		tstr   = '1976-07-01 12:34:56.789012'
		format = '%Y-%m-%d %H:%M:%S.%f'
		self.in_birthdate = time.mktime(time.strptime(tstr, format))

		self.in_x = 5
		self.in_y = 3

		self.in_a = 42


		#--- expected outputs

		self.out_age = time.time() - self.in_birthdate

		self.out_sum = 8
		self.out_diff = 2

		self.out_c = 44


	#--- low-level extension calls

	def test_extension_one_to_one(self):
		e = x_age()

		age = e(self.in_birthdate)[0]

		self.assertAlmostEqual(age, self.out_age, 0,
			"a one-to-one extension (using Extension class directly) did not result in the expected numerical value"
		)

	def test_extension_many_to_many(self):
		e = x_math()

		sum, diff = e(self.in_x, self.in_y)

		self.assertEqual(sum, self.out_sum,
			"a many-to-many extension (using Extension class directly) resulted in at least one bad value"
		)
		self.assertEqual(diff, self.out_diff,
			"a many-to-many extension (using Extension class directly) resulted in at least one bad value"
		)


	#--- fundamental LazyDict operation

	def test_instantiation(self):
		#most simple
		d = ExampleLazyDict(name=self.in_name)

		#with additional data
		d = ExampleLazyDict(name=self.in_name, birthdate=self.in_birthdate)

	def test_getitem_extension_one_to_one(self):
		d = ExampleLazyDict(name=self.in_name, birthdate=self.in_birthdate)

		self.assertAlmostEqual(d['age'], self.out_age, 0,
			"a one-to-one extension did not result in the expected numerical value"
		)

	def test_getitem_extension_many_to_many(self):
		d = ExampleLazyDict(name=self.in_name, x=self.in_x, y=self.in_y)

		self.assertEqual(d['sum'], self.out_sum,
			"a many-to-many extension resulted in at least one bad value"
		)
		self.assertEqual(d['diff'], self.out_diff,
			"a many-to-many extension resulted in at least one bad value"
		)

	def test_getitem_extension_indirect(self):
		d = ExampleLazyDict(a=self.in_a)

		try:
			self.assertEqual(d['c'], self.out_c,
				"an indirect extension resulted in an unexpected value"
			)
		except KeyError:
			raise AssertionError("indirect extension did not work")

	def test_contains_through_extension(self):
		d = ExampleLazyDict(a=self.in_a)

		self.assertTrue('c' in d)

	def test_has_key_through_extension(self):
		d = ExampleLazyDict(a=self.in_a)

		self.assertTrue(d.has_key('c'))

	def test_getitem_not_available_no_source(self):
		#note no birthdate, therefore not possible to compute age by extension
		d = ExampleLazyDict(name=self.in_name)
		self.assertRaises(KeyError, d.__getitem__, 'age')

	def test_getitem_not_available_extension_not_providing_it(self):
		d = ExampleLazyDict(name=self.in_name)
		self.assertRaises(KeyError, d.__getitem__, 'never')

	def test_getitem_unexpected_key(self):
		d = ExampleLazyDict(name=self.in_name)
		self.assertRaises(KeyError, d.__getitem__, 'this-is-not-a-known-key')

	def test_getitem_identity_extension(self):
		d = ExampleLazyDict(name=self.in_name)
		self.assertEqual(d['name'], d['name_copy'],
			"a simple identity extension did not work as expected"
		)


	#--- performance and laziness

	def test_getitem_extension_done_once(self):
		"""Test that the an extension is computed only once."""
		d = ExampleLazyDict(name=self.in_name, birthdate=self.in_birthdate)

		count = d._extension_count  #(class var, so this will have been jacked up by other instances)

		d['age']
		self.assertEqual(
			d._extension_count,
			count+1,  #i.e. one more
			"extension execution count, after the first extension, is not what was expected"
		)
		d['age']
		self.assertEqual(
			d._extension_count,
			count+1,  #i.e. no change
			"extension execution count, after additional getitem, is not what was expected"
		)

	def test_getitem_extension_count_query_optimized(self):
		"""Test that the an extension is computed only once."""
		d = ExampleLazyDict(name=self.in_name, x=self.in_x, y=self.in_y)
		d['_laziness'] = lazydict.LAZINESS_QUERY_OPTIMIZED  #(the default)

		count = d._extension_count  #(class var, so this will have been jacked up by other instances)

		d['sum']  #this should store 'diff', too
		self.assertEqual(
			d._extension_count,
			count+1,  #i.e. one more
			"extension execution count, when query-optimized, after the first extension, is not what was expected"
		)

		d['diff']  #this should have already been there
		self.assertEqual(
			d._extension_count,
			count+1,  #i.e. no more
			"extension execution count, when query-optimized, after additional getitem, is not what was expected"
		)

	def test_getitem_extension_count_data_optimized(self):
		"""Test that the an extension is computed only once."""
		d = ExampleLazyDict(name=self.in_name, x=self.in_x, y=self.in_y)
		d['_laziness'] = lazydict.LAZINESS_DATA_OPTIMIZED  #(the default)

		count = d._extension_count  #(class var, so this will have been jacked up by other instances)

		d['sum']  #this should NOT store 'diff', too
		self.assertEqual(
			d._extension_count,
			count+1,  #i.e. one more
			"extension execution count, when data-optimized, after the first extension, is not what was expected"
		)

		d['diff']  #this will have to re-run the extension
		self.assertEqual(
			d._extension_count,
			count+2,  #i.e. one more
			"extension execution count, when data-optimized, after additional getitem, is not what was expected"
		)


if __name__=='__main__':
	unittest.main()
