# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""plotting with matplotlib"""

from dio import processor

DEFAULT_FIGURE_SIZE = (5,5)

@processor
def pie(label_k=None, value_k=None, out=None, err=None):
	"""Make a pie chart."""

	import matplotlib.pyplot as plt

	#settings
	figsize = DEFAULT_FIGURE_SIZE  #(w,h) in inches
	threshold_percentage = None  #None or a number in the range (0,100)
	other_label = 'OTHER'  #applicable iff threshold_percentage is not None

	#the data accumulator
	pie = {}

	try:
		while True:
			d = yield
			if label_k is not None and value_k is not None:
				pie[d[label_k]] = d[value_k]
			else:
				for k, v in d.iteritems():
					pie[k] = v
	except GeneratorExit:
		if len(pie) > 0:
			#collapse small slices into one, if applicable
			if threshold_percentage is not None:
				total_value = sum(pie.values())
				threshold_value = threshold_percentage * total_value / 100.

				other_value = 0
				for key in pie.keys():
					if pie[key] < threshold_value:
						other_value += pie.pop(key)

				if other_value > 0:
					pie[other_label] = other_value

			#sort order for the slices
			def piecmp(x, y):
				"""cmp for sorting pie slices.

				Both x and y are tuples (key, value).

				This sorts by value, except puts the 'OTHER' entry last
				regardless of its value.
				"""
				if x[0] == other_label:
					return -1
				if y[0] == other_label:
					return 1
				return cmp(x[1], y[1])

			#numeric labels on the slices
			def percent(x):
				"""Convert float x to a string label."""
				return '%d%%' % (x + 0.5)

			#convert to matplotlib's format
			labels, values = zip(*sorted(pie.iteritems(), piecmp))


			#--- actual plotting

			fig = plt.figure(figsize=figsize)
			ax = fig.gca()

			#http://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.pie
			ax.pie(
				values,
				labels=labels,
				autopct=percent,
			)

			##no legend option, yet
			#ax.legend()

			plt.show()

@processor
def histogram(out=None, err=None):
	"""Make a histogram."""

	import numpy as np
	import matplotlib.pyplot as plt

	#settings
	figsize = DEFAULT_FIGURE_SIZE  #(w,h) in inches
	nbins = 10

	#the data accumulator(s)
	values = {}

	try:
		while True:
			d = yield
			for k in d:
				try:
					values[k].append(d[k])
				except KeyError:
					values[k] = [d[k],]
	except GeneratorExit:
		for k in values.keys():
			#using numpy.histogram instead of pyplot.hist directly so we get
			#numbers for textual output, too

			hist, hist_bin_edges = np.histogram(values[k], nbins)

			bin_width = (hist_bin_edges[-1]-hist_bin_edges[0])/len(hist)


			#--- actual plotting
			fig = plt.figure(figsize=figsize)
			ax = fig.gca()

			#http://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.bar
			ax.bar(
				hist_bin_edges[:-1],
				hist,
				width=bin_width,
			)

			#I don't like how the highest bar is flush with the plot top by default
			ax.set_ylim(top=1.1*max(hist))

			plt.show()
