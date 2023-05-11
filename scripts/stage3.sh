#!/bin/bash
rm -r models/best_model_rf.model
rm -r models/best_model_lr.model
spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar scripts/model.py
