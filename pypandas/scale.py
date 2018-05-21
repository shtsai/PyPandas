#!/use/bin/env/python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import FloatType
   
def load_test():
    print("Load scale functions successfully.")

def unpack_vector(dataFrame, column, featureNames):
    if type(featureNames) is not list: 
        raise ValueError("The featureNames has to be a list.")
    unpack = udf(lambda x, index: float(x[index]), FloatType())
    # Recursive helper function
    buf = []
    def helper(column, buf, featureNames, index):
        if index == 0:
            return [unpack(col(column), lit(index)).alias(featureNames[index])]
        else:
            return helper(column, buf, featureNames, index - 1) + [unpack(col(column), lit(index)).alias(featureNames[index])]
    dataFrame = dataFrame.select([x for x in dataFrame.columns] + [*helper(column, buf, featureNames, len(featureNames) - 1)]).drop(column)
    return dataFrame

def getAssembledDataFrame(dataFrame, inputColNames):
    assembler = VectorAssembler(inputCols=inputColNames,\
                                outputCol="features")
    assembledDF = assembler.transform(dataFrame)
    return assembledDF

def standard_scale(dataFrame, inputColNames, usr_withStd=True, usr_withMean=False):
    
    assembledDF = getAssembledDataFrame(dataFrame, inputColNames)
    scaler=StandardScaler(inputCol="features", \
                          outputCol="scaled features", \
                          withStd=usr_withStd, \
                          withMean=usr_withMean).fit(assembledDF)
    scaledDF = scaler.transform(assembledDF)
    return scaledDF

def min_max_scale(dataFrame, inputColNames, Min=0.0, Max=1.0):

    assembledDF = getAssembledDataFrame(dataFrame, inputColNames)
    scaler=MinMaxScaler(inputCol="features", \
                        outputCol="scaled features")
    scaler.setMax(Max).setMin(Min)
    scalerModel=scaler.fit(assembledDF)
    scaledDF = scalerModel.transform(assembledDF)
    return scaledDF

def max_abs_scale(dataFrame, inputColNames):
    
    assembledDF = getAssembledDataFrame(dataFrame, inputColNames)
    scaler=MaxAbsScaler(inputCol="features",\
                        outputCol="scaled features")
    scalerModel=scaler.fit(assembledDF)
    scaledDF = scalerModel.transform(assembledDF).drop("features")
    return scaledDF

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
        colList = ""
        for inputColName in inputColNames:
            colList += " '" + inputColName + "' "
        if(p_norm == float('inf')):
            print ("Successfully assembled the column {0:s} to a feature vector and normalized using L^inf norm and create two new columns 'features' and 'normalized features'.".format(colList))
        else:
            print ("Successfully assembled the column {0:s} to a feature vector and normalized using L^{1:f} norm and create two new columns 'features' and 'normalized features'.".format(colList, p_norm))
        return normalizedDF
    else:
        raise ValueError("The inputColNames has to be a list of columns to generate a feature vector and then do normalization.")

