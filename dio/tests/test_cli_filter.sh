#!/usr/bin/env bash
set -e

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

echo '
{"foo": 2}
{"foo": 4}
{"foo": 6}
{"foo": 8}
' | dio.filter %foo -gt 5
#{"foo": 6}
#{"foo": 8}

echo '
{"foo": 42, "bar": 99}
' | dio.filter %foo -gt 10 -a \( %bar -eq "some string" -o True \)
#{"foo": 42, "bar": 99}
