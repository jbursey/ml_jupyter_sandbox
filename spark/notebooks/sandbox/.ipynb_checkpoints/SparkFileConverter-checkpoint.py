import pyspark
import os
import datetime
import uuid
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

class SparkFileConverter:
    def __init__(self):
        return
    
    def init_spark(self, master_loc="spark://172.25.0.9:7077"):
        conf = SparkConf()
        if master_loc != "":
            print("Setting master loc: {master_loc}".format(master_loc=master_loc))
            conf.setMaster(master_loc)
        self.spark = SparkSession.builder.appName("file_converter").config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("INFO")
        return
    
    def convert_files(self, source_loc_pattern, output_loc, input_compression = "gzip", ouptut_compression = "gzip"):        
        df = {}
        
        read = self.spark.read
        read.option("header", True)
        read.option("inter_schema", True)
        if input_compression == "gzip":
            print("input_compression = gzip")
            read.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
            df = read.csv(source_loc_pattern)            
        elif input_compression == "csv":     
            print("input_compression = csv")
            df = read.csv(source_loc_pattern)
        elif input_compression == "snappy":
            print("input_compression = snappy")
            read.option("codec", "org.apache.spark.io.SnappyCompressionCodec")
            df = read.text(source_loc_pattern)
        
        #-------- output
        df.repartition(4000)      
        if ouptut_compression == "gzip":
            print("ouptut_compression = gzip")
            df.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
            df.write.csv(output_loc)
        elif ouptut_compression == "csv":     
            print("ouptut_compression = csv")
            #df.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
            df.write.csv(output_loc)
        elif ouptut_compression == "snappy":
            print("ouptut_compression = snappy")                 
            df.write.option("codec", "org.apache.spark.io.SnappyCompressionCodec").text(output_loc)
        
          
        
            
                    