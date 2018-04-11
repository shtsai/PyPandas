from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler 

class Kmeans():
    '''Perform KMean clustering on the given columns of the dataframe'''
    def __init__(self, k=3):
        self.k = k

    def fit(self, df, columns):
        '''Run KMean clustering with the features'''
        df_with_features = self.create_features(df, columns)

        # Append features and predictions to the original dataframe
        km = KMeans(k=self.k)
        self.model = km.fit(df_with_features)
        self.newdf = self.model.transform(df_with_features)
        self.summary = self.model.summary

    def create_features(self, df, columns):
        '''Use Vector Assembler to create a new column that contains a vector of features'''
        if type(columns) is str:
            columns = [columns]
        
        assembler = VectorAssembler(inputCols=columns, outputCol="features")
        return assembler.transform(df)
    
    def summary(self):
        return self.summary

    def cluster_centers(self):
        return self.model.clusterCenters()

    def cluster_sizes(self):
        return self.summary.clusterSizes

    def new_dataframe(self):
        return self.newdf
