import sys, time
from pypandas.textCleaner import clean_text, sub_with_pattern

def clean(df):
    starttime = time.time()
    df = sub_with_pattern(df, '*', '!"#$%&/()=?', '')
    df.show(20, False)
    print("Our textcleaner takes: " + str(time.time() - starttime) + " sec to clean the columns.")

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
