#!/usr/bin/env bash

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

input_foo_foo_bar='
{"name":"foo"}
{"name":"foo"}
{"name":"bar"}
'

echo "$input_foo_foo_bar" | dio.identity
#{"name": "foo"}
#{"name": "foo"}
#{"name": "bar"}

echo "$input_foo_foo_bar" | dio.uniq
#{"name": "foo"}
#{"name": "bar"}
