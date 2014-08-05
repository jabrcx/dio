#!/usr/bin/env bash

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

input='
{"foo": 8}
{"bar": 8}
{"zzz": 8}
{"foo": 8}
{"bar": 8}
{"foo": 8}
{"x": 1}
{"y": 1}
'

echo "$input" | dio.plot.pie
