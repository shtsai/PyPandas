#!/bin/bash
/usr/bin/spark-submit scaler_experiment_311.py standard_scale
/usr/bin/spark-submit scaler_experiment_311.py min_max_scale
/usr/bin/spark-submit scaler_experiment_311.py max_abs_scale
/usr/bin/spark-submit scaler_experiment_311.py normalize
