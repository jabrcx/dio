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

	BUGS/TODO
		Only direct extensions are currently supported.  That is, if you have 
		the two extensions `a->b' and `b->c', just having an `a' will not allow 
		`c' to be computed.  The design intention is that this should work.
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

		#for what to optimize
		self._laziness = LazyDict.LAZINESS_QUERY_OPTIMIZED

		#whether or not to update existing data when new data is presented as a 
		#side-effect of computing extensions
		self._overwrite = True

	def __getitem__(self, key):
		"""__getitem__
		
		This method itself is rather optimized for LAZINESS_QUERY_OPTIMIZED, as 
		it uses try/except instead of if/hasattr, which assumes success will be 
		more frequent than failure.

		FIXME: This needs a whole lot of work and should be properly 
		formalized.  See the BUGS/TODO in the main class doc.
		"""
		
		try:
			return dict.__getitem__(self, key)
		except KeyError:
			if self._laziness == LazyDict.LAZINESS_LOCKED:
				raise
			else:
				fulfilled = False
				for e in self.extensions:
					if key in e.target and all([ dict.has_key(self, sk) for sk in e.source ]):
						LazyDict._extension_count += 1
						for k, v in zip(e.target, e(*[ dict.__getitem__(self, sk) for sk in e.source ])):
							if v is not None:
								if k == key:
									self[k] = v
									fulfilled = True
								elif self._laziness == LazyDict.LAZINESS_QUERY_OPTIMIZED:
									if self._overwrite:
										self[k] = v
									else:
										if not dict.has_key(self, k):
											self[k] = v
								else:
									if fulfilled: break
				return dict.__getitem__(self, key)  #may still raise KeyError

	def set_laziness(self, laziness):
		"""Set this instance's laziness.

		:param laziness: the new laziness setting
		:type laziness: one of the class LAZINESS* values
		"""
		self._laziness = laziness
	
	def set_overwrite(self, overwrite):
		"""Set whether or not new data should overwrite exiting data.

		:param overwrite: the new overwrite setting
		:type overwrite: bool
		"""
		self._overwrite = overwrite
