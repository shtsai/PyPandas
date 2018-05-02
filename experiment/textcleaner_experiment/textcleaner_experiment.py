import sys, time
from pypandas.textCleaner import clean_text, sub_with_pattern

starttime = time.time()
df = load_data_311("aws")
df = sub_with_pattern(df, '*', '!"#$%&/()=?', '')
df.show(20, False)
print("Our textcleaner takes: " + str(time.time() - starttime) + " sec to clean the columns.")
