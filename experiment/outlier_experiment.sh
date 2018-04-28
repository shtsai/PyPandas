#!/bin/bash

spark-submit outlier_experiment.py kmeans 1 3
spark-submit outlier_experiment.py kmeans 3 3
spark-submit outlier_experiment.py kmeans 5 3
spark-submit outlier_experiment.py kmeans 10 3

spark-submit outlier_experiment.py kmeans 1 4
spark-submit outlier_experiment.py kmeans 3 4
spark-submit outlier_experiment.py kmeans 5 4
spark-submit outlier_experiment.py kmeans 10 4

spark-submit outlier_experiment.py kmeans 1 5
spark-submit outlier_experiment.py kmeans 3 5
spark-submit outlier_experiment.py kmeans 5 5
spark-submit outlier_experiment.py kmeans 10 5
