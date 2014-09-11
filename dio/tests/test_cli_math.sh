#!/usr/bin/env bash
set -e

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

echo '
{"value": 24}
{"value": 16}
{"value":  8}
{"value":  1}
{"value":  1}
' | dio.count
#{"value": 5}

echo '
{"value": 24}
{"value": 16}
{"value":  8}
{"value":  1}
{"value":  1}
' | dio.sum
#{"value": 50}

echo '
{"value": 24}
{"value": 16}
{"value":  8}
{"value":  1}
{"value":  1}
' | dio.average


#--- --groupby

##not implemented yet
#echo '
#{"name: "foo", "value": 8}
#{"name: "bar", "value": 8}
#{"name: "zzz", "value": 8}
#{"name: "foo", "value": 8}
#{"name: "bar", "value": 8}
#{"name: "foo", "value": 8}
#{"name:   "x", "value": 1}
#{"name:   "y", "value": 1}
#' | dio.sum --groupby name
