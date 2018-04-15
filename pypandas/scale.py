#!/use/bin/env/python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.feature import Normalizer
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

def normalize(dataFrame, inputColNames, p_norm=2.0):
    if type(p_norm) is str:
        if p_norm.lower() == "inf":
            p_norm = float('inf')
        else:
            raise ValueError("The p_norm has to be float or 'inf'.")
    if type(inputColNames) is list:
        outputColName = "normalized features"
        assembler = VectorAssembler(inputCols=inputColNames, \
                outputCol="features")
        assembledDF = assembler.transform(dataFrame)
        normalizer=Normalizer(inputCol="features", \
            outputCol=outputColName, \
            p = p_norm)
        normalizedDF = normalizer.transform(assembledDF)
        colList=""
        for inputColName in inputColNames:
            colList += " '"+inputColName+"' "
        if(p_norm == float('inf')):
            print ("Successfully assembled the column {0:s} to a feature vector and normalized using L^inf norm and create two new columns 'feature' and 'normalized feature'."\
                .format(colList))
        else:
            print ("Successfully assembled the column {0:s} to a feature vector and normalized using L^{1:f} norm and create two new columns 'feature' and 'normalized feature'."\
                .format(colList, p_norm))
        return normalizedDF
    else:
         raise ValueError("The inputColNames has to be a list of columns to generate a feature vector and then do normalization.")

