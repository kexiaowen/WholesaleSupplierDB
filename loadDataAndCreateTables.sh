#!/bin/bash

python processData.py
/temp/apache-cassandra-3.11.3/bin/cqlsh -f createTable.txt
/temp/apache-cassandra-3.11.3/bin/cqlsh -f createMaterializedViews.txt
/temp/apache-cassandra-3.11.3/bin/cqlsh -f loadData.txt