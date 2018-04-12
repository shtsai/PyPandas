#!/use/bin/env/python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

def load_test():
    print("Load scale functions successfully.")

def stander_scale(dataFrame, inputColNames, outputColName, usr_withStd=True, usr_withMean=False):
    if type(inputColNames) is str:
        assembler = VectorAssembler(inputCols=[inputColNames], \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        scaler=StandardScaler(inputCol="features", \
                outputCol=outputColName, \
                withStd=usr_withStd, \
                withMean=usr_withMean \
                ).fit(assembledDF)
        scaledData = scaler.transform(assembledDF)
        return scaledData
    elif type(inputColNames) is list:
        assembler = VectorAssembler(inputCols=inputColNames, \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        scaler=StandardScaler(inputCol="features", \
                outputCol=outputColName, \
                withStd=usr_withStd, \
                withMean=usr_withMean \
                ).fit(assembledDF)
        scaledData = scaler.transform(assembledDF)
        return scaledData
    else:
         raise ValueError("The inputColNames has to be string or string list.")

