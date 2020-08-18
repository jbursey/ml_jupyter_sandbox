import pyspark
import os
import datetime
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

class GpsSparkLogic:
    def __init__(self):
        mpe = "987M";
        pyspark_submit_args = ' --executor-memory ' + mpe + ' pyspark-shell'
        os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
        
        return

    def init_spark(self, master_location = "spark://172.25.0.09:7077"):
        #self.spark = SparkSession.builder.appName("gps_spark_cubiq").getOrCreate()
        confg = pyspark.SparkConf()
        if master_location != "":            
            print("setting master to: ", master_location)
            confg.setMaster(master_location)
        self.context = pyspark.SparkContext(conf=confg)        
        self.sqlContext = SQLContext(self.context)
        #self.sqlContext.hadoopConfiguration.set("fs.s3.access.key", "awsaccesskey value")
        
        #self.sqlContext.hadoopConfiguration.set("fs.s3.secret.key", "aws secretkey value")
        #self.sqlContext.hadoopConfiguration.set("fs.s3.endpoint", "s3.amazonaws.com")        

    def read_data(self, csvPathPattern = "/opt/spark/sample_data2/daily-feed-raw/*.csv"):        
        schema = StructType([StructField('tz_timestamp',IntegerType(), True),
                    StructField('device_id', StringType(), True),
                    StructField('os', IntegerType(), True),
                    StructField('latitude', FloatType(), True),
                    StructField('longitude', FloatType(), True),
                    StructField('accuracy', IntegerType(), True),
                    StructField('row_number', IntegerType(), True),
                    StructField('anchor', IntegerType(), True)
                    ])        
        
        self.df = self.sqlContext.read \
        .option("delimeter", '\t') \
        .option("sep", '\t') \
        .option("inferSchema", False) \
        .option("header", False) \
        .schema(schema) \
        .csv(csvPathPattern)
        
        #part-00000-c4806039-81a8-4ca2-a65b-deaaa9ea491f-c000.csv = 212066
        #part-00001-c4806039-81a8-4ca2-a65b-deaaa9ea491f-c000.csv = 239077
        #part-00002-c4806039-81a8-4ca2-a65b-deaaa9ea491f-c000.csv = 244955        

    def show_df_stats(self):
        print("count: ", self.df.count())
        self.df.show(10, False)
        subdf = self.df.select(self.df.columns[0:2])
        subdf.show(10, False)
    
    def process_data(self):
        
        subset = self.df.filter((self.df.accuracy >= 5) & (self.df.accuracy <= 20))
        subset.show(50, False)
        print("subset accuracy count: ", subset.count())
        
        subset = subset.filter((subset.latitude >= 31.0) & (subset.latitude <= 33.0) & (subset.longitude >= -98.0) & (subset.longitude <= -97.0))
        subset.show(50, False)
        print("subset geo count: ", subset.count())
        
        today_str = "{:%m_%d_%Y}".format(datetime.date.today())
        subset \
        .coalesce(2) \
        .write \
        .csv("/opt/spark/sample_data/daily-feed-reduced/" + today_str, sep='\t')
        #    .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \

        return
    
    def save_test_s3_file(self, filePathPattern = "/opt/spark/sample_data/daily-feed-reduced/"):
        test_data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        rdd = self.context.parallelize(test_data)
        df = rdd.toDF()
        df.show(100, False)
        
        today_str = "{:%m_%d_%Y}-{uuid}".format(datetime.date.today(), uuid=uuid.uuid1().hex)
        
        df \
        .coalesce(3) \
        .write \
        .csv(filePathPattern + today_str, sep='\t')
        
