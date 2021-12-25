from pyspark.sql import SparkSession
import pyspark
import numpy as np
from math import sqrt, exp
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StandardScaler
from pyspark.ml import Pipeline

#starting the spark context for future operations
sc = pyspark.SparkContext()
spark = SparkSession(sc)

'''
global array for silhouette scores
'''
silhouette_score=[]

'''
evaluator for clustering definition
'''
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

# Loads data.
dataset = sc.textFile("features.csv")

'''
load features into spark
'''
features = dataset.map(lambda l: (l.split(',')[0], float(l.split(',')[1]), float(l.split(',')[2]), float(l.split(',')[3]), float(l.split(',')[4])))


'''
define DF schema for easy operations and labelling
'''
schema = StructType([StructField("address", StringType(), False), StructField("avg_gas_price", DoubleType(), False), StructField("avg_gas", DoubleType(), False), StructField("block_number", DoubleType(), False), StructField("total_value", DoubleType(), False)])

'''
conversion to DF
'''
featuresDF = features.toDF(schema)

'''
feature vector creation
'''
assemble=VectorAssembler(inputCols=[
 'avg_gas_price',
 'avg_gas',
 'block_number',
 'total_value',
], outputCol='features')

'''
fit features DF to the features vector
'''
assembled_data=assemble.transform(featuresDF)

'''
scales the features to a standardized scale (may not be efficient - zscore may be better)
'''
scale=StandardScaler(inputCol='features',outputCol='standardized')

'''
fit data into standardized values
'''
data_scale=scale.fit(assembled_data)

'''
add values to old DF
'''
data_scale_output=data_scale.transform(assembled_data)

'''
run kmeans elbow algorithm
'''
for i in range(2,10):
    
    KMeans_algo=KMeans(featuresCol='standardized', k=i)
    
    KMeans_fit=KMeans_algo.fit(data_scale_output)
    
    output=KMeans_fit.transform(data_scale_output)
    
    
    
    score=evaluator.evaluate(output)
    
    silhouette_score.append(score)
    
    print("Silhouette Score:",score)
