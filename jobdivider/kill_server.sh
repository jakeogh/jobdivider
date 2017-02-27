#!/bin/sh

kill -9 `lsof -i4 -P | grep 'localhost:5555 (LISTEN)' | ~/cfg/text/collapse_whitespace | cut -d ' ' -f 2`
