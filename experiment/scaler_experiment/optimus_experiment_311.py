import sys, time
import optimus as op
from pypandas.datasets import *

columns = ["Unique Key", "Incident Zip", "X Coordinate (State Plane)", "Y Coordinate (State Plane)", "Latitude", "Longitude"]

def test_min_max_scale():
    df = load_data_311("aws")
    starttime = time.time()
    transformer = op.DataFrameTransformer(df)
    transformer.scale_vec_col(columns, 'scaled')
    transformer.df.count()
    print("The optimus min_max_scale() takes: " + str(time.time() - starttime) + " sec.")

def test_normalize():
    df = load_data_311("aws")
    starttime = time.time()
    transformer = op.DataFrameTransformer(df)
    transformer.normalizer(columns)
    transformer.df.count()
    print("The optimus normalize() takes: " + str(time.time() - starttime) + " sec.")

def main(): 
    func = sys.argv[1]
    if func == "min_max_scale":
        test_min_max_scale()
    elif func == "normalize":
        test_normalize()
    else:
        raise ValueError("Invalid argument.")

if __name__ == "__main__":
    main()
