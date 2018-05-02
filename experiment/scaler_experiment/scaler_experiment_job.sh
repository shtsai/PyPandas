#!/bin/bash
/usr/bin/spark-submit scaler_experiment_job.py standard_scale
/usr/bin/spark-submit scaler_experiment_job.py min_max_scale
/usr/bin/spark-submit scaler_experiment_job.py max_abs_scale
/usr/bin/spark-submit scaler_experiment_job.py normalize
