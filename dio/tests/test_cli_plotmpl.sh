#!/usr/bin/env bash

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

echo '
{"foo": 8}
{"bar": 8}
{"zzz": 8}
{"foo": 8}
{"bar": 8}
{"foo": 8}
{"x": 1}
{"y": 1}
' | dio.plot.pie

echo '
{"foo": 0.5}
{"foo": 1.5}
{"foo": 1.5}
{"foo": 2.5}
{"foo": 2.5}
{"foo":	2.5}
{"foo": 3.5}
{"foo": 3.5}
{"foo": 3.5}
{"foo": 3.5}
{"foo": 4.5}
{"foo": 4.5}
{"foo": 4.5}
{"foo": 4.5}
{"foo": 4.5}
{"foo": 5.5}
{"foo": 5.5}
{"foo": 5.5}
{"foo": 5.5}
{"foo": 5.5}
{"foo": 6.5}
{"foo": 6.5}
{"foo": 6.5}
{"foo": 6.5}
{"foo": 7.5}
{"foo": 7.5}
{"foo": 7.5}
{"foo": 8.5}
{"foo": 8.5}
{"foo": 9.5}
' | dio.plot.histogram
