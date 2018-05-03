import sys, time
import optimus as op
import pypandas.datasets import *

def clean(df):
    starttime = time.time()
    transformer = op.DataFrameTransformer(df)
    transformer.remove_special_chars(columns='*')
    transformer.show(20, False)
    print("The optimus takes: " + str(time.time() - starttime) + " sec to clean the columns.")

def load_data():
    data = sys.argv[1]
    if data == "job":
        df = load_data_job("aws")
        print("[data_job]: ")
    elif data == "311":
        df = load_data_311("aws")
        print("[data_311]: ")
    elif data == "permit":
        df = load_data_permit("aws")
        print("[data_permit]: ")
    else:
        raise ValueError("Invalid argument.")
    return df

def main():
    df = load_data()    
    clean(df)

if __name__ == "__main__":
    main()
