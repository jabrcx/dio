# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""a dict with transparent, on-demand computation and optimizable memoization

LazyDict is for datasets of associative arrays where the presence of some 
key/value pairs allows for the derivation of other key/value pairs (e.g. 
through a direct computation, a system call, a database lookup, or some other 
system interaction).  Data relationships are captured as simple, isolated 
`Extensions' -- functions that compute a set of values for target keys given 
a set of values for the source keys.  Defining a LazyDict class is just a 
matter of defining a set of such Extensions.  Using a LazyDict is the same as 
using a regular dict.

Internally, key values are memoized to avoid recomputation when reused.  The 
memoization can be configured to optimize for query count (aggressively 
prefetch and cache data at every query opportunity), to optimize for memory 
size (cache only the values for the keys explicitly used), or to `lock' the 
instance such that no more data is extended.  It can also be configured to 
overwrite or keep existing data when new data is presented.  Keeping with the 
pure dict interface, this memoization is configured through normal dict 
key/value pairs (`_laziness', and `_overwrite').

The concept of extensions as used here originated in egg, authored by Saul 
Youssef and John A. Brunelle.

.. moduleauthor:: John A. Brunelle <john_brunelle@harvard.edu>
"""


import logging
from itertools import repeat, izip, chain


#--- laziness settings: how to handle extra available data

#note there is currently no setting which allows data to be computed without 
#memoizing it; these settings control what to do extra available data.

LAZINESS_LOCKED = 0  #the data are considered complete -- no more extensions 
                     #will be attempted

LAZINESS_DATA_OPTIMIZED = 1  #data size is minimized, by not storing any extra 
                             #values beyond what's explicitly used

LAZINESS_QUERY_OPTIMIZED = 2  #queries are minimized, by prefetching and 
                              #storing as much data as possible per query

DEFAULT_LAZINESS = LAZINESS_QUERY_OPTIMIZED


#--- overwrite settings: how to handle when new values appear for existing data

OVERWRITE_UPDATE = 0  #replace the existing values with the new ones

OVERWRITE_PRESERVE = 1  #preserve the existing values (throw away the new ones)

DEFAULT_OVERWRITE = OVERWRITE_UPDATE


class Extension(object):
	"""A computation of target data given source data.

	To implement an Extension, define two class-variable tuples of keys -- the 
	source set and the target set -- and implement __call__, to compute the 
	tuple of target values given the source values.  If any target key is 
	non-computable, return None in place of its value.
	"""

	source = ()  #a tuple of keys
	target = ()  #a tuple of keys

	def __repr__(self):
		return '<(%s)->(%s)>' % (','.join(self.source), ','.join(self.target))

	def __call__(self, *args):
		"""Return the target values.

		:param args: the source values

		:rtype: tuple (NOTE: possibly one-item -- if target is one-item)

		If any target value is not available or computable, return None in its 
		place.  The default implementation is the identity (literally -- not 
		copies).
		"""
		return args

class LazyDict(dict):
	"""a dict with transparent, on-demand computation and optimizable memoization

	See the module documentation for the general overview.

	To implement a LazyDict, simply inherit from this base class and define the 
	class variable `extensions', a list of Extension instances.  The order 
	matters -- extensions are attempted roughly in order.  It's also extremely 
	good practice to document the expected keys and their value types in the 
	class doc.

	__getattr__ and related methods will raise KeyError if the data is not 
	present, such as:

		* the key(s) necessary to compute the values through extensions are not 
		  present

		* there is no extension that computes the value for that key

		* data extension is limited by the laziness setting

	There are two `special' keys:

		_laziness: the laziness setting (see module doc)

		_overwrite: the overwrite setting (see module doc)

	Key values are never None -- that special value is used internally during 
	extension evaluation.  This might change, we'll see what problems we run 
	into...
	"""


	#--- subclasses should set this

	extensions = []  #a list of Extension instances


	#--- internal telemetry

	_extension_count = 0  #number of times extensions have been called


	def has_key(self, key):
		"""dict's has_key, with transparent LazyDict semantics.

		Note: has_key() is deprecated in Python; use the `in' operator, i.e. 
		__contains__(), instead.
		"""
		return key in self

	def __contains__(self, key):
		"""dict's __contains__, with transparent LazyDict semantics."""
		try:
			self[key]
		except KeyError:
			return False
		return True
	
	def __getitem__(self, key):
		"""dict's __getitem__, with transparent LazyDict semantics.

		Note that this is recursive, and there is no guarantee of termination.  
		If extensions create loops, this may recurse infinitely.

		This method itself is rather optimized for LAZINESS_QUERY_OPTIMIZED, as 
		it usually uses EAFP (try/except) over of LBYL (if/hasattr), which 
		assumes success will be more frequent than failure.

		TODO:
			* The code assumes no extensions set _laziness and _overwrite.  
			  That should be supported, as it could be useful.
		"""

		try:
			#EAFP -- hopefully we already have it, in which case just return it
			return dict.__getitem__(self, key)
		except KeyError:
			if dict.get(self,'_laziness',DEFAULT_LAZINESS) == LAZINESS_LOCKED:
				#if extending is not allowed, we don't have it, and we're done
				raise
			else:
				#try to compute it through extensions

				fulfilled = False  #if we've satisfied the main request
				
				e1 = []  #candidate extensions where we already have all sources, w/o further extending
				         #try these first, since they are the most efficient, and to avoid infinite loops
				e2 = []  #candidate extensions where one or more of the sources are only available through extension, if at all
				         #invoking these can cause recursion, which may be infinite
				
				#sort the extensions into the above two categories (original order is preserved within a category)
				for e in self.extensions:
					if key in e.target:
						if all([ (dict.__contains__(self, sk)) for sk in e.source ]):  #using dict, so no extending, no recursion
							e1.append(e)
						else:
							e2.append(e)

				#try each candidate extension
				for sources_known_present, e in chain(izip(repeat(True), e1), izip(repeat(False), e2)):
					if sources_known_present or all([ (sk in self) for sk in e.source ]):  #(sk in self) makes this recursive and allows for extension chaining
						#we have all the required source values; run the extension
						logging.getLogger('dio.lazydict.extension').debug(repr(e))
						LazyDict._extension_count += 1
						for k, v in izip(e.target, e(*[ dict.__getitem__(self, sk) for sk in e.source ])):
							if v is not None:
								if k == key:
									#this is the key we want -- store the value
									self[k] = v
									fulfilled = True
									if dict.get(self,'_laziness',DEFAULT_LAZINESS) == LAZINESS_DATA_OPTIMIZED:
										#we have what we need and don't care about the rest of the target data
										break
								elif dict.get(self,'_laziness',DEFAULT_LAZINESS) == LAZINESS_QUERY_OPTIMIZED:
									#this is not the key that we want, but store the value so we don't have to re-query
									#but only if it's not already there or the instance is configured to update existing data
									if not dict.__contains__(self,k) or dict.get(self,'_overwrite',DEFAULT_OVERWRITE) == OVERWRITE_UPDATE:
										self[k] = v
					if fulfilled: break
				
				return dict.__getitem__(self, key)  #(may still raise KeyError)
