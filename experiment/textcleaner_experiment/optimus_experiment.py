import sys, time
import optimus as op
import pypandas.datasets import *

starttime = time.time()
df = load_data_311("aws")
transformer = op.DataFrameTransformer(df)
transformer.remove_special_chars(columns='*')
transformer.select.show(20, False)
print("The optimus takes: " + str(time.time() - starttime) + " sec to clean the columns.")
