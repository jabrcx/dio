# Copyright (c) 2013-2014
# John A. Brunelle
# Harvard FAS Research Computing
# All rights reserved.

"""a dict enhanced with on-demand computation and query vs. cache optimization

LazyDict is designed for working with datasets where each data object is 
described by key/value pairs and certain key/value pairs are derived from other 
ones, e.g. through a simple computation, a system call, a lookup in a database, 
or some other system interaction.  LazyDict's purpose is to encapsulate the set 
of such data relationships -- called `extensions' -- and work universally with 
such data objects -- through, and only through, the normal dict interface.  

Under the hood, each instance's `_laziness' setting (just another key/value 
pair) can be used to customize whether to optimize for query count 
(aggressively pre-fetch and cache data at every query opportunity), to optimize 
for memory size (cache only the values for the keys explicitly used), or to 
`lock' the instance such that no more data is extended.  There is also an 
`_overwrite' setting, used to determine what to do with new values for existing 
data.

Several of the concepts here originated in egg, authored by Saul Youssef and 
John A. Brunelle.

.. moduleauthor:: John A. Brunelle <john_brunelle@harvard.edu>
"""


#--- laziness settings: how to handle extra available data

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

	def __str__(self):
		return '<(%s)->(%s)>' % (','.join(self.source), ','.join(self.target))

	def __call__(self, *args):
		"""Return the target values.

		:param args: the source values

		:rtype: tuple (NOTE: possibly one-item -- if target is one-item)

		If any target value is not available or computable, return None.
		The default implementation is the identity (literally -- not copies).
		"""
		return args

class LazyDict(dict):
	"""a dict enhanced with on-demand computation and query vs. cache optimization

	See the module documentation for the general overview.

	To implement a LazyDict, simply inherit from this base class and define the 
	class variable `extensions', a list of Extension instances.  It's also 
	extremely good practice to document the expected keys and value types in 
	the class doc.

	__getattr__() and related methods will raise KeyError if the data is not 
	present, such as:

		* the key(s) necessary to compute the values through extensions are not 
		  present

		* there is no extension that computes the value for that key

		* data extension is limited by the laziness setting

	There are two `special' keys:

		_laziness: the laziness setting (see module doc)

		_overwrite: the overwrite setting (see module doc)

	Values are never None -- that special value is used internally during 
	extension evaluation.  This might change, we'll see what problems we run 
	into...
	"""


	#--- subclasses should set this

	extensions = []  #a list of Extension instances


	#--- internal telemetry

	_extension_count = 0  #number of times extensions have been called


	def has_key(self, key):
		"""dict's has_key, enhanced with laziness and extension functionality.

		Note: has_key() is deprecated in Python; use the `in' operator, i.e. 
		__contains__(), instead.
		"""
		return key in self

	def __contains__(self, key):
		"""dict's __contains__, enhanced with laziness and extension functionality."""
		try:
			self[key]
		except KeyError:
			return False
		return True

	def __getitem__(self, key):
		"""dict's __getitem__, enhanced with laziness and extension functionality.

		This method itself is rather optimized for LAZINESS_QUERY_OPTIMIZED, as 
		it uses try/except instead of if/hasattr, which assumes success will be 
		more frequent than failure.

		Note that this is recursive, and there is no guarantee of termination.  
		Specifically, if extensions create loops, this may recurse infinitely.

		TODO:
		The code assumes no extensions set _laziness and _overwrite.  That 
		should be supported, as it could be useful.
		"""

		try:
			return dict.__getitem__(self, key)
		except KeyError:
			if dict.get(self,'_laziness',DEFAULT_LAZINESS) == LAZINESS_LOCKED:
				raise
			else:
				fulfilled = False
				for e in self.extensions:
					if key in e.target and all([ (sk in self) for sk in e.source ]):  #(recursion)
						LazyDict._extension_count += 1
						for k, v in zip(e.target, e(*[ dict.__getitem__(self, sk) for sk in e.source ])):
							if v is not None:
								if k == key:
									self[k] = v
									fulfilled = True
								elif dict.get(self,'_laziness',DEFAULT_LAZINESS) == LAZINESS_QUERY_OPTIMIZED:
									if dict.get(self,'_overwrite',DEFAULT_OVERWRITE) == OVERWRITE_UPDATE:
										self[k] = v
									else:
										if not dict.__contains__(self,k):
											self[k] = v
								else:
									if fulfilled: break
						if fulfilled: break
				return dict.__getitem__(self, key)  #may still raise KeyError
