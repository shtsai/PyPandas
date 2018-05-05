import sys, time 
from pypandas.datasets import *
from pypandas.textCleaner import * 

def clean(df): 
    df = sub_with_pattern(df, '*', '!"#$%&/()=?', '') 
    df.count()

def load_data(): 
    data = sys.argv[1] 
    if data == "job": 
        df = load_data_job("aws") 
    elif data == "311": 
        df = load_data_311("aws") 
    elif data == "permit": 
        df = load_data_permit("aws") 
    else: 
        raise ValueError("Invalid argument.") 
    return df 

def main(): 
    starttime = time.time() 
    df = load_data() 
    clean(df) 
    print("Our textcleaner takes: " + str(time.time() - starttime) + " sec to clean the data " + sys.argv[1]) 
    
if __name__ == "__main__": 
    main()
