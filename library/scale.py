#!/use/bin/env/python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def load_test():
    print("Load scale functions successfully.")

def standard_scale(dataFrame, inputColNames, usr_withStd=True, usr_withMean=False):
    if type(inputColNames) is str:
        outputColName = "scaled " + inputColNames
        assembler = VectorAssembler(inputCols=[inputColNames], \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        scaler=StandardScaler(inputCol="features", \
                outputCol=outputColName, \
                withStd=usr_withStd, \
                withMean=usr_withMean \
                ).fit(assembledDF)
        scaledDF = scaler.transform(assembledDF).drop("features")
        castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
        scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
        print ("Successfully scale the column '{0:s}' and create a new column '{1:s}'.".format(inputColNames, outputColName))
        return scaledDF
    elif type(inputColNames) is list:
        scaledDF = dataFrame
        for inputColName in inputColNames:
            outputColName = "scaled " + inputColName
            assembler = VectorAssembler(inputCols=[inputColName], \
                    outputCol="features")
            assembledDF = assembler.transform(scaledDF)
            scaler=StandardScaler(inputCol="features", \
                    outputCol=outputColName, \
                    withStd=usr_withStd, \
                    withMean=usr_withMean \
                    ).fit(assembledDF)
            scaledDF = scaler.transform(assembledDF).drop("features")
            castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
            scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
            print ("Successfully scale the column '{0:s}' and create a new column '{1:s}'.".format(inputColName, outputColName))
        return scaledDF
    else:
         raise ValueError("The inputColNames has to be string or string list.")

