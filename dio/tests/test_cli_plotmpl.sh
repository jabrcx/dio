#!/usr/bin/env bash
set -e

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

echo '
{"name": "foo", "value": 24}
{"name": "bar", "value": 16}
{"name": "zzz", "value":  8}
{"name":   "x", "value":  1}
{"name":   "y", "value":  1}
' | dio.plot.pie name value

#this is not the normal data model! but it works, for convenience
echo '
{"foo": 24}
{"bar": 16}
{"zzz": 8}
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
