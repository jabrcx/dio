# Copyright (c) 2013-2014
# Harvard FAS Research Computing
# All rights reserved.

"""LazyDict"""


class Extension(object):
	"""Compute target data given source data."""
	source = ()  #a tuple of keys
	target = ()  #a tuple of keys
	
	def __str__(self):
		return '<(%s)->(%s)>' % (','.join(self.source), ','.join(self.target))
	
	def __call__(self, *args):
		"""Return the target values.

		:param args: the source values

		:rtype: tuple (NOTE: possibly one-item -- if target is one-item)

		If any target value is not available or computable, return None.
		The default implementation is the identity (and not copies).
		"""
		return args

class LazyDict(dict):
	"""A dict-like object for lazily computing data only when needed.

	The term `laziness' refers to optimization -- whether this should optimized 
	to reduce data usage, queries, or something else.  The current options are:
	
		* LAZINESS_DATA_OPTIMIZED
		
		  Data size is minimized, by not storing any extra attributes beyond 
		  what's necessary.

		* LAZINESS_QUERY_OPTIMIZED
		
		  Queries are minimized, by prefetching and storing as much data as 
		  possible per query.

		* LAZINESS_LOCKED
		
		  The data are considered complete -- no more queries will be issued.

	getattr() will raise KeyError if the data is not present, such as:

		* the key(s) necessary to compute the value are not present
		* data extension is limited by the laziness setting
		* the given key is not valid
	
	Values are never None -- that special value is used internally during the 
	extension evaluation.  This might change, we'll see what problems we run 
	into...
	"""

	LAZINESS_LOCKED = 0
	LAZINESS_DATA_OPTIMIZED = 1
	LAZINESS_QUERY_OPTIMIZED = 2


	#--- subclasses should set this, if there are any

	extensions = []  #a list of Extension instances


	#--- internal telemetry

	_extension_count = 0  #number of times an extension has been called


	def __init__(self, *args, **kwargs):
		dict.__init__(self, *args, **kwargs)

		#for what to optimize; do NOT pop off this key
		self['_laziness'] = LazyDict.LAZINESS_QUERY_OPTIMIZED

		#whether or not to update existing data when new data is presented as a 
		#side-effect of computing extensions; do NOT pop off this key
		self['_overwrite'] = True
	
	def has_key(self, key):
		"""dict's has_key, enhanced with laziness and extension functionality.

		Note: has_key() is deprecated in Python; use the `in' operator, i.e. __contains__(), instead.
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
		"""
		
		try:
			return dict.__getitem__(self, key)
		except KeyError:
			if dict.__getitem__(self,'_laziness') == LazyDict.LAZINESS_LOCKED:
				raise
			else:
				fulfilled = False
				for e in self.extensions:
					if key in e.target and all([ (sk in self) for sk in e.source ]):
						LazyDict._extension_count += 1
						for k, v in zip(e.target, e(*[ dict.__getitem__(self, sk) for sk in e.source ])):
							if v is not None:
								if k == key:
									self[k] = v
									fulfilled = True
								elif dict.__getitem__(self,'_laziness') == LazyDict.LAZINESS_QUERY_OPTIMIZED:
									if dict.__getitem__(self,'_overwrite'):
										self[k] = v
									else:
										if not dict.__contains__(k):
											self[k] = v
								else:
									if fulfilled: break
						if fulfilled: break
				return dict.__getitem__(self, key)  #may still raise KeyError
