#!/use/bin/env/python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import MaxAbsScaler
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

def min_max_scale(dataFrame, inputColNames, Min=0.0, Max=1.0):
    if type(inputColNames) is str:
        outputColName = "scaled " + inputColNames
        assembler = VectorAssembler(inputCols=[inputColNames], \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        scaler=MinMaxScaler(inputCol="features", \
                outputCol=outputColName)
        scaler.setMax(Max)\
              .setMin(Min)
        scalerModel=scaler.fit(assembledDF)
        scaledDF = scalerModel.transform(assembledDF).drop("features")
        castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
        scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
        print ("Successfully scale the column '{0:s}' to range ({1:f}, {2:f}) and create a new column '{3:s}'."\
                .format(inputColNames,scaler.getMin(),scaler.getMax(), outputColName))
        return scaledDF
    elif type(inputColNames) is list:
        scaledDF = dataFrame
        for inputColName in inputColNames:
            outputColName = "scaled " + inputColName
            assembler = VectorAssembler(inputCols=[inputColName], \
                    outputCol="features")
            assembledDF = assembler.transform(scaledDF)
            scaler=MinMaxScaler(inputCol="features", \
                    outputCol=outputColName)
            scaler.setMax(Max)\
                  .setMin(Min)
            scalerModel=scaler.fit(assembledDF)
            scaledDF = scalerModel.transform(assembledDF).drop("features")
            castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
            scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
            print ("Successfully scale the column '{0:s}' to range ({1:f}, {2:f}) and create a new column '{3:s}'."\
                    .format(inputColName,scaler.getMin(),scaler.getMax(), outputColName))

        return scaledDF
    else:
         raise ValueError("The inputColNames has to be string or string list.")

def max_abs_scale(dataFrame, inputColNames):
    if type(inputColNames) is str:
        outputColName = "scaled " + inputColNames
        assembler = VectorAssembler(inputCols=[inputColNames], \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        scaler=MaxAbsScaler(inputCol="features", \
                outputCol=outputColName)
        scalerModel=scaler.fit(assembledDF)
        scaledDF = scalerModel.transform(assembledDF).drop("features")
        castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
        scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
        print ("Successfully scale the column '{0:s}' to range (-1, 1) and create a new column '{1:s}'."\
                .format(inputColNames, outputColName))
        return scaledDF
    elif type(inputColNames) is list:
        scaledDF = dataFrame
        for inputColName in inputColNames:
            outputColName = "scaled " + inputColName
            assembler = VectorAssembler(inputCols=[inputColName], \
                    outputCol="features")
            assembledDF = assembler.transform(scaledDF)
            scaler=MaxAbsScaler(inputCol="features", \
                    outputCol=outputColName)
            scalerModel=scaler.fit(assembledDF)
            scaledDF = scalerModel.transform(assembledDF).drop("features")
            castVectorToFloat = udf(lambda v : float(v[0]), FloatType())
            scaledDF = scaledDF.withColumn(outputColName, castVectorToFloat(outputColName)) 
            print ("Successfully scale the column '{0:s}' to range (-1, 1) and create a new column '{1:s}'."\
                    .format(inputColName, outputColName))

        return scaledDF
    else:
         raise ValueError("The inputColNames has to be string or string list.")


