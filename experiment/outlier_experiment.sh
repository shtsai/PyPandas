#!/bin/bash

/usr/bin/spark-submit outlier_experiment.py kmeans 1 3
/usr/bin/spark-submit outlier_experiment.py kmeans 3 3
/usr/bin/spark-submit outlier_experiment.py kmeans 5 3
/usr/bin/spark-submit outlier_experiment.py kmeans 10 3

/usr/bin/spark-submit outlier_experiment.py kmeans 1 4
/usr/bin/spark-submit outlier_experiment.py kmeans 3 4
/usr/bin/spark-submit outlier_experiment.py kmeans 5 4
/usr/bin/spark-submit outlier_experiment.py kmeans 10 4

/usr/bin/spark-submit outlier_experiment.py kmeans 1 5
/usr/bin/spark-submit outlier_experiment.py kmeans 3 5
/usr/bin/spark-submit outlier_experiment.py kmeans 5 5
/usr/bin/spark-submit outlier_experiment.py kmeans 10 5
