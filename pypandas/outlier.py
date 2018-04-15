from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler 
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
import math
from library.preprocess import *

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

def init():
    df = load_data_job()
    km = KMeansOutlierRemover(5)
    km.fit(df, "Initial Cost")
    km.summary()
    return km

class KMeansOutlierRemover:
    '''Perform KMean clustering on the given columns of the dataframe'''
    def __init__(self, k=3):
        self.k = k
        self.km = KMeans(k=self.k)

    def _udf_compute_distance(self, centers): 
        '''This wrapper function returns an user defined function for computing distance in cluster'''
        def _compute_distance(features, prediction):
            center = centers[prediction]
            dist = 0.0
            for i in range(len(features)):
                dist += pow(features[i] - center[i], 2)
            return math.sqrt(dist)

        return udf(_compute_distance, DoubleType())

    def fit(self, df, columns):
        '''Run KMean clustering with the features'''
        df_with_features = self.create_features(df, columns)

        self.model = self.km.fit(df_with_features)
        # Append features and predictions to the original dataframe
        newdf = self.model.transform(df_with_features)        
        self._summary = self.model.summary
        
        centers = self.model.clusterCenters()
        compute_distance = self._udf_compute_distance(centers)
        self.df = newdf.withColumn("distance to cluster center", compute_distance("features","prediction"))

    def create_features(self, df, columns):
        '''Use Vector Assembler to create a new column that contains a vector of features'''
        if type(columns) is str:
            columns = [columns]
        self.columns = columns
       
        assembler = VectorAssembler(inputCols=columns, outputCol="features")
        return assembler.transform(df)

    def show_cluster(self, cluster_index):
        '''Display a summary of the given cluster, including distance and cluster features'''
        columns = ["prediction", "distance to cluster center"]
        columns.extend(self.columns)
        self.df.where(col("prediction") == cluster_index).select(columns).show()

    def filter(self, cluster_index, distance):
        '''Filter out rows which are too far away from its cluster center'''
        self.df = self.df.where(~((col("prediction") == cluster_index) & (col("distance to cluster center") > distance)))

    def summary(self):
        '''Show summary of the clustering and provide information to help filter outliers'''
        # Compute average distance to cluster center
        data = self.df.select(["prediction", "distance to cluster center"])
        data = data.withColumnRenamed("prediction", "cluster index")
        avg = data.groupBy("cluster index").agg({"distance to cluster center": "avg"})
 
        # format cluster sizes
        size = self.cluster_sizes()
        size = [[i, size[i]] for i in range(len(size))]
        size_df = spark.createDataFrame(size, ["cluster index","size"])

        # format cluster centers
        center = self.cluster_centers()
        center = [[i, float(center[i][0])] for i in range(len(center))]
        center_df = spark.createDataFrame(center, ["cluster index","cluster center"])
        
        result = size_df.join(center_df, "cluster index").join(avg, "cluster index").orderBy("cluster index")
        result.show()
        return result

    def cluster_centers(self):
        return self.model.clusterCenters()

    def cluster_sizes(self):
        return self._summary.clusterSizes

    def get_dataframe(self):
        return self.df
