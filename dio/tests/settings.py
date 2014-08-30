# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.


if False:
	import logging
	for name in ('dio',):
		logger = logging.getLogger(name)
		logger.setLevel(logging.DEBUG)
		handler = logging.StreamHandler()
		formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(name)s: %(message)s')
		handler.setFormatter(formatter)
		logger.addHandler(handler)
