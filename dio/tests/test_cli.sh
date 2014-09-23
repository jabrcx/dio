#!/usr/bin/env bash
set -e

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.


#--- misc basics (and testers needed for those)

echo "test dio.identity"
echo '
{"name": "foo"}
{"name": "foo"}
{"name": "bar"}
' | dio.identity
#{"name": "foo"}
#{"name": "foo"}
#{"name": "bar"}

echo "test EPIPE recovery"
./eg.send_infinite | head -n 2 | wc -l
#2

echo "test dio.tidy"
echo '
{"foo": 1, "bar": 2, "zzz": 3}
{"foo": 1, "bar": 2, "zzz": 3}
' | dio.tidy bar
#{"bar": 2}
#{"bar": 2}

#sum must be computed through extension, since eg.send_one does not include it
./eg.send_one | dio.tidy sum
#{"sum": 8, "__class__": "eglib.ExampleLazyDict"}

echo "test eg.cli_of_python_pipeline"
echo '
{"x": 10, "y": 25}
{"x": 15, "y": 20}
{"x": 20, "y": 15}
{"x": 25, "y": 10}
' | ./eg.cli_of_python_pipeline | dio.tidy x
#{"x": 15}
#{"x": 20}


#--- coreutils

echo "test dio.sort"
echo '
{"foo": 3}
{"foo": 5}
{"foo": 1}
' | dio.sort foo
#{"foo": 1}
#{"foo": 3}
#{"foo": 5}

echo "test dio.uniq"
echo '
{"name": "foo"}
{"name": "foo"}
{"name": "bar"}
' | dio.uniq
#{"name": "foo"}
#{"name": "bar"}

echo "test dio.wc"
echo '
{"name": "foo"}
{"name": "foo"}
{"name": "bar"}
' | dio.wc
#{"count": 3}


#--- math

echo "test dio.count"
echo '
{"foo": 1}
{"bar": 2}
{"zzz": 3}
{"foo": 4}
{"bar": 5}
{"foo": 6}
' | dio.count | dio.sort  #need to sort to make output surely deterministic
#{"zzz": 1}
#{"bar": 2}
#{"foo": 3}

echo "test dio.sum"
echo '
{"foo": 1}
{"bar": 2}
{"zzz": 3}
{"foo": 4}
{"bar": 5}
{"foo": 6}
' | dio.sum | dio.sort  #need to sort to make output surely deterministic
#{"zzz": 3}
#{"bar": 7}
#{"foo": 11}
