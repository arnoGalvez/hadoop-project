#!/usr/bin/bash

echo $1 | hdfs dfs -appendToFile - /user/$USER/$2