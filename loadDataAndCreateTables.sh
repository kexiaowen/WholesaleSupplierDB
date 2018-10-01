#!/bin/bash

./cqlsh -f loadData.txt
./cqlsh -f createTable.txt
./cqlsh -f createMaterializedViews.txt