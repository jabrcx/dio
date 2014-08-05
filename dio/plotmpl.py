# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""plotting with matplotlib"""

from dio import processor

@processor
def pie(out=None, err=None):
	"""Make a pie chart."""

	#settings
	figsize = (5,5)  #(w,h) in inches
	threshold_percentage = 4  #None or a number in the range (0,100)
	other_label = 'OTHER'  #applicable iff threshold_percentage is not None

	#the data accumulator
	pie = {}

	try:
		while True:
			d = yield
			for k in d:
				try:
					pie[k] += d[k]
				except KeyError:
					pie[k] = d[k]
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

			#convert to mpl's format
			labels, values = zip(*sorted(pie.items(), piecmp))

			#actual plotting
			#see: http://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.pie
			import matplotlib.pyplot as plt
			fig = plt.figure(figsize=figsize)
			ax = fig.gca()
			ax.pie(values, labels=labels, autopct=percent)
			ax.pie(values, labels=labels)
			#ax.legend()
			plt.show()
