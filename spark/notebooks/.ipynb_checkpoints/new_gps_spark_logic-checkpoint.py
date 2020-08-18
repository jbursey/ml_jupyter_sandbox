import pyspark
import os
import datetime
import uuid
import math
import operator
import pygeohash as pgh
import boto3
import re
import calendar
import time as t
from datetime import (datetime, date, time, timedelta)
from pyspark.sql.window import Window
from pyspark.sql.functions import (when, udf, col, lit, lead, lag, row_number)
from pyspark.sql import (SparkSession, DataFrameReader)
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as py
from sys import exit
from tabulate import tabulate

def time_ms():
    return int(round(t.time() * 1000))

def s3_dates(object='daily-feed'):
    '''
    Returns a list of GPS dates present in S3
    Options include: daily-feed, daily-feed-reduced, micro-clusters, home, work
    '''
    
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    gps_bucket = 'b6-8f-fc-09-0f-db-50-3f-gpsdata'
    
    if object == 'daily-feed':
        gps_prefix = 'cuebiq/daily-feed/US/'
    elif object == 'daily-feed-reduced':
        gps_prefix = 'cuebiq/daily-feed-reduced/US/'  
    elif object == 'micro-clusters':
        gps_prefix = 'cuebiq/processed-data/US/micro-clusters/'
    elif object == 'home':
        gps_prefix = 'cuebiq/processed-data/US/home/20'
    elif object == 'work':
        gps_prefix = 'cuebiq/processed-data/US/work/20'
    else:
        return

    dates = set()
    for result in paginator.paginate(Bucket=gps_bucket, Prefix=gps_prefix, Delimiter='/'):
        for prefix in result.get('CommonPrefixes'):
            dates.add(datetime.strptime((re.search("(\d{6,})", prefix.get('Prefix')).group() + '01')[0:8], '%Y%m%d'))
    return sorted(list(dates))

def anchor_dist(coord1, coord2, dist_thresh=430, unit='feet'):
    lat1 = coord1[0]
    lon1 = coord1[1]
    lat2 = coord2[0]
    lon2 = coord2[1]

    
    return lon1
    
    

def distance(lat1, lon1, lat2, lon2, unit='miles'):
    '''
    Measure simple haversine distance between two points. Default unit = miles.
    '''    
    units = {'miles':3963.19,'kilometers':6378.137,'meters':6378137,'feet':20902464}    

    phi_1 = py.radians(lat1)
    phi_2 = py.radians(lat2)
    delta_phi = py.radians(lat2 - lat1)
    delta_lambda = py.radians(lon2-lon1)

    area = py.sin(delta_phi/2.0) ** 2 \
    + py.cos(phi_1) * py.cos(phi_2) * \
    py.sin(delta_lambda / 2.0) ** 2

    central_angle = 2 * py.asin((area ** 0.5))
    radius = units[unit.lower()]

    return py.abs(py.round((central_angle * radius),4))

def rate_of_speed(distance, seconds, time_unit='hour'):
    '''
    Returns the rate of speed based on input distance and seconds
    '''
    time = {'second':1, 'minute':60, 'hour':3600, 'day':86400, 'year':31536000}
    return py.abs(distance / (seconds / time[time_unit]))
    
class GpsPingFilter:
    def __init__(self):
        #mpe = "2800M"
        #pyspark_submit_args = ' --executor-memory ' + mpe + ' pyspark-shell'
        #os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
        return

    def init_spark(self, master_location = "spark://172.25.0.09:7077"):        
        print("init_spark called")
        confg = pyspark.SparkConf()
        if master_location != "":            
            print("setting master to: ", master_location)
            confg.setMaster(master_location)
        self.spark = SparkSession.builder.config(conf=confg).appName("clean_and_cluster").getOrCreate()        
        self.spark.udf.register("distance",distance,FloatType())
        self.spark.udf.register('rate_of_speed',rate_of_speed, FloatType())        
        return
        
    def load_daily_feed(self, source='s3', start_dt=None, end_dt=None):
        '''
        Load daily-feed pings from specified dates.
        If dates left blank, program will automatically determine study date(s)
        "Source" options include 's3' or 'local'
        '''
        
        df_days = s3_dates('daily-feed')
        dfr_days = s3_dates('daily-feed-reduced')

        if start_dt is not None and end_dt is not None:
            study_dt = []
            diff = datetime.strptime(end_dt, '%Y-%m-%d') - datetime.strptime(start_dt, '%Y-%m-%d')
            for dt in range(diff.days+1):
                study_dt.append(datetime.strptime(start_dt, '%Y-%m-%d') + timedelta(days=dt))
            self.study_dts = sorted(study_dt)
        
        elif start_dt is not None and end_dt is None:
            self.study_dts = [datetime.strptime(start_dt, '%Y-%m-%d')]
                
        else:
            self.study_dts = sorted(list(set(df_days) - set(dfr_days) - set([max(df_days)])))

        
        dt_universe = set()
        for dt in self.study_dts:
            dt_universe.add(dt + timedelta(days=-1))
            dt_universe.add(dt)
            dt_universe.add(dt + timedelta(days=1))
            
        # Exit script if a study date is empty or date(s) for analysis don't exist  
        if len(set(dt_universe) - set(df_days)) > 0:
            exit("Not all dates needed for analysis exist in the daily-feed")
        
        if len(self.study_dts) == 0:
            exit("There are no dates to process currently")

        # Create needed file paths (local or S3)
        if source == 's3':
            base_path = 's3://b6-8f-fc-09-0f-db-50-3f-gpsdata/cuebiq/daily-feed/US/'
        else:
            base_path = "/opt/spark/sample_data2/daily-feed/US/"
            
        dt_paths = []
        for dt in dt_universe:
            dt_paths.append(base_path + str(dt).replace('-','')[0:8] + '*/*.csv.gz')
        print(dt_paths)

        df_schema = StructType([StructField("utc_timestamp",IntegerType(),True),
                    StructField("device_id",StringType(),True),
                    StructField("os",IntegerType(),True),
                    StructField("latitude",FloatType(),True),
                    StructField("longitude",FloatType(),True),
                    StructField("accuracy",IntegerType(),True),
                    StructField("tz_offset",IntegerType(),True)])

        self.df = self.spark.read \
            .option("sep", '\t') \
            .option("inferSchema", False) \
            .option("header", False) \
            .schema(df_schema) \
            .csv(",".join(dt_paths).split(','))

        self.df = self.df.repartition(2000)
        
        print("Number of partitions {partitions}".format(partitions = self.df.rdd.getNumPartitions()))
        
        print("\nRunning the following date(s) ({:,} total):".format(len(self.study_dts)))
        for dt in sorted(self.study_dts):
            print("   " + dt.strftime('%x'))
            
        print("\n{:,} pings loaded for analysis".format(self.df.count()))
            
    def junk_filter(self, offset=240, should_count = True):
        '''
        junk_filter(integer,boolean) -> void
        offset = time in seconds from delta time
        should_count = show debug counts at end of filter pass...peformance hit
        '''
        print("\n_______________________________________________\nJUNK FILTER\n\n")
        
        start_ms = time_ms()
        
        init_cnt_df = {}
        null_cnt_df = {}
        dupe_cnt_df = {}
        acc_cnt_df = {}
        coord_cnt_df = {}
        fnl_df = {}
        
        
        # Apply daily time offset in minutes (offset at 4am instead of midnight)
        offset = 240
        for index,dt in enumerate(self.study_dts):
            self.study_dts[index] = dt + timedelta(minutes=offset)
        
        # Convert study dates into unix timestamps
        unix_dts = []
        for dt in self.study_dts:
            unix_dts.append(calendar.timegm(dt.timetuple()))
                
        init_cnt_df = self.df        
        
        # Drop records with null values in critical fields
        self.df = self.df.na.drop(subset=['latitude','longitude','utc_timestamp','tz_offset','accuracy'])        
        null_cnt_df = self.df        
        
        # Drop duplicate records based on device_id and timestamp
        self.df = self.df.dropDuplicates(['utc_timestamp','device_id'])        
        dupe_cnt_df = self.df        
        
        # Remove records falling outside safe horizontal accuracy thresholds
        self.df = self.df.filter((self.df.accuracy >= 5) & (self.df.accuracy <= 65))        
        acc_cnt_df = self.df        
        
        # Remove records falling outside of a bounding rectanlge of the contintental US, AK, and HI
        self.df = self.df.filter(((self.df.latitude >= 23.82585) & (self.df.latitude <= 50.107813) \
                                 & ((self.df.longitude >= -125.821901) & (self.df.longitude <= -65.934603))) \
                                 | ((self.df.latitude >= 50.494424) & (self.df.latitude <= 72.113805) \
                                 & ((self.df.longitude >= 172) | (self.df.longitude <= -128))) \
                                 | ((self.df.latitude >= 18.186832) & (self.df.latitude <= 26.499983) \
                                 & (self.df.longitude >= -172.536313) & (self.df.longitude <= -154.039891)))
                
        coord_cnt_df = self.df        
        
        # Remove records falling outside of the study range scope
        schema = StructType([StructField("utc_timestamp",IntegerType(),True),
                                StructField("device_id",StringType(),True),
                                StructField("os",IntegerType(),True),
                                StructField("latitude",FloatType(),True),
                                StructField("longitude",FloatType(),True),
                                StructField("accuracy",IntegerType(),True),
                                StructField("tz_offset",IntegerType(),True),
                                StructField("study_dt", StringType(),True)])   

        fnl_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(),schema) 

        for dt in unix_dts:
            fnl_df = fnl_df.union(self.df.filter((self.df['utc_timestamp'] + self.df['tz_offset']).between(dt, dt + 86399)) \
                                  .withColumn("study_dt",py.to_date(py.from_unixtime(lit(dt)))))                    
        
        self.df = fnl_df
        
        init_cnt = -1
        null_cnt = -1
        dupe_cnt = -1
        acc_cnt = -1
        coord_cnt = -1           
        tm_cnt = -1
        
        if should_count is True:                        
            print("init_cnt")
            init_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            init_cnt_df.foreach(lambda row: init_cnt_accumulator.add(1))            
            #init_cnt = init_cnt_df.select("accuracy").count()
            init_cnt = init_cnt_accumulator.value
            
            print("null_cnt")
            null_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            null_cnt_df.foreach(lambda row: null_cnt_accumulator.add(1))      
            #null_cnt = null_cnt_df.select("accuracy").count()
            null_cnt = null_cnt_accumulator.value
            
            print("dupe_cnt")
            dupe_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            dupe_cnt_df.foreach(lambda row: dupe_cnt_accumulator.add(1))      
            #dupe_cnt = dupe_cnt_df.select("accuracy").count()
            dupe_cnt = dupe_cnt_accumulator.value
            
            print("acc_cnt")
            acc_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            acc_cnt_df.foreach(lambda row: acc_cnt_accumulator.add(1))      
            #acc_cnt = acc_cnt_df.select("accuracy").count()
            acc_cnt = acc_cnt_accumulator.value
            
            print("coord_cnt")
            coord_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            coord_cnt_df.foreach(lambda row: coord_cnt_accumulator.add(1))      
            #coord_cnt = coord_cnt_df.select("accuracy").count()        
            coord_cnt = coord_cnt_accumulator.value
            
            print("tm_cnt")
            tm_cnt_accumulator = self.spark.sparkContext.accumulator(0)
            fnl_df.foreach(lambda row: tm_cnt_accumulator.add(1))      
            #tm_cnt = fnl_df.select("accuracy").count()
            tm_cnt = tm_cnt_accumulator.value

        else:
            print("fnl_df complete junk_filter")
            
        tbl_data = [['Initial count', init_cnt, 0, 0, 'Count of pings before junk filtering process'], \
           ['Null values', null_cnt, init_cnt - null_cnt, ((init_cnt - null_cnt) / float(init_cnt)) * 100, \
            'Empty values among latitude, longitude, accuracy, timestamp, tz offset'], \
           ['Duplicates', dupe_cnt, null_cnt - dupe_cnt, ((null_cnt - dupe_cnt) / float(init_cnt)) * 100, \
            'Records with duplicate device_id and utc_timestamps'], \
           ['Accuracy', acc_cnt, dupe_cnt - acc_cnt, ((dupe_cnt - acc_cnt) / float(init_cnt)) * 100, \
            'Horizontal accuracy values exceeding safe thresholds (outside of 5 - 65)'], \
           ['Coordinates', coord_cnt, acc_cnt - coord_cnt, ((acc_cnt - coord_cnt) / float(init_cnt)) * 100, \
            'Pings occurring outside bounding rectangles of the continental US, AK, and HI'], \
           ['Study date(s)', tm_cnt, coord_cnt - tm_cnt, ((coord_cnt - tm_cnt) / float(init_cnt)) * 100, \
            'Pings occuring outside the study date range when tz_offset is applied'], \
           ['Final count', tm_cnt, init_cnt - tm_cnt, ((init_cnt - tm_cnt) / float(init_cnt)) * 100, \
            'Count of pings after junk filtering process']]

        end_ms = time_ms()
        
        print(tabulate(tbl_data, floatfmt=".2f", headers=['Phase', 'Ping Count', 'Removed Pings', \
                                                          'Percent Reduction', 'Description']))
        print("junk_filter time ms: {ms}".format(ms = end_ms - start_ms))
    
    def speed_filter(self):
        print("\n_______________________________________________\nSPEED FILTER\n\n")
        
        w = Window().partitionBy('device_id', 'study_dt').orderBy('utc_timestamp')
        init_cnt = self.df.count()

        self.df = self.df.withColumn('dist_to',distance(self.df['latitude'], self.df['longitude'], \
                                    lead(self.df['latitude'],1).over(w), lead(self.df['longitude'],1).over(w))) \
            .withColumn('sec_to', (lead(self.df['utc_timestamp'], 1).over(w) - self.df['utc_timestamp'])) \
            .withColumn('speed_to', rate_of_speed(col('dist_to'), col('sec_to'),'hour')) \
            .withColumn('dist_from', lag(col('dist_to'), 1).over(w)) \
            .withColumn('sec_from', lag(col('sec_to'), 1).over(w)) \
            .withColumn('speed_from', lag(col('speed_to'), 1).over(w)) \
            .withColumn('speed_flag', when(((col('dist_to').isNull()) | (col('dist_from').isNull())) \
                         | ((((col('speed_from') + col('speed_to')) / 2) <= 90) | ((col('dist_to') >= 150) \
                                                                                   | (col('dist_from') >= 150))) \
                         & ((col('speed_from') < 600) & (col('speed_to') < 600)) \
                         & ((col('speed_from') < 30) | (col('speed_to') < 30)), 1).otherwise(0)) 
            
        # Calculate average movement speed of pings to be removed
        speed_df = self.df.filter(self.df['speed_flag'] == 0) \
                .withColumn('avg_speed', (self.df['speed_from'] + self.df['speed_to']) / 2)       

        # Collect min and max average speeds as variables
        min_speed = speed_df.agg(py.min(col('avg_speed'))).collect()[0][0]
        max_speed = speed_df.agg(py.max(col('avg_speed'))).collect()[0][0]
        
        # Bucket pings into different speed ranges and incorporate min and max 
        sum_df = speed_df.withColumn('speed_bucket', when(col('avg_speed') < 50, str(min_speed) + " - 49.9") \
                                    .when((col('avg_speed') >= 50) & (col('avg_speed') < 60), "50 - 59.9") \
                                    .when((col('avg_speed') >= 60) & (col('avg_speed') < 70), "60 - 69.9") \
                                     .when((col('avg_speed') >= 70) & (col('avg_speed') < 80), "70 - 79.9") \
                                     .when((col('avg_speed') >= 80) & (col('avg_speed') < 90), "80 - 89.9") \
                                     .when((col('avg_speed') >= 90) & (col('avg_speed') < 100), "90 - 99.9") \
                                     .when(col('avg_speed') >= 100, "100 - " + str(max_speed)) \
                                     .otherwise(lit("other")))
       
        # Collect row objects for table display
        speed_tbl = sum_df.groupBy(sum_df['speed_bucket']).count().collect()

        # Parse out values within row objects to format and sort table
        tab_row = []
        for row in speed_tbl:
            tab_row.append([float(re.search("(\S+)", row[0]).group()), \
                            float(re.search("[-]\s(\S+)", row[0]).group(1)), \
                           row[1]])    

        # Filter out flagged pings for main dataframe and clean up fields
        self.df = self.df.filter(self.df['speed_flag'] == 1) \
                .drop('dist_to', 'dist_from', 'sec_to', 'speed_to', 'sec_from', 'speed_from', 'speed_flag')
        
       # Count of final pings remaining after speed filter
        #spd_cnt = self.df.cache().count()
        spd_cnt = self.df.count()

        
        tbl_data = [['Initial count', init_cnt, 0, 0, 'Count of pings before applying speed filter'], \
           ['Final count', spd_cnt, init_cnt - spd_cnt, ((init_cnt - spd_cnt) / float(init_cnt)) * 100, \
            'Count of pings after applying speed filter']]

        # Display overall filter
        print(tabulate(tbl_data, floatfmt=".2f", headers=['Phase', 'Ping Count', 'Removed Pings', \
                                                          'Percent Reduction', 'Description']))
        
        # Display bucketed filter
        print("\n\n\n")
        print(tabulate(sorted(tab_row), headers=['Minimum Average (mph)', 'Maximum Average (mph)', 'Pings Removed']))      
        

    def linear_filter(self):
        print("\n_______________________________________________\nLINEAR MOVEMENT FILTER\n\n")
        
        init_cnt = self.df.count()

        # Create various partitions and sortings for downstream window functions
        w = Window().partitionBy('device_id', 'study_dt').orderBy('utc_timestamp')
        l = Window().partitionBy('device_id', 'study_dt', 'lin_grp').orderBy('utc_timestamp')
        
        # Number of pings to analyze in a group to determine linearity
        lgrp = 4 

        self.df = self.df.withColumn('RecordNum',row_number().over(w)) \
            .withColumn('lin_grp', py.ceil(row_number().over(w) / lgrp)) \
            .withColumn('dist_to', distance(self.df['latitude'], self.df['longitude'], \
                  lead(self.df['latitude'],1).over(l), lead(self.df['longitude'],1).over(l),'meters')) \
            .withColumn('sequence', row_number().over(l))


        # Create aggregated table for linear groupings
        expr = [py.min(col('utc_timestamp')).alias('min_utc_timestamp'), \
                py.max(col('utc_timestamp')).alias('max_utc_timestamp'), \
                py.count(col('utc_timestamp')).alias('cnt'), \
                py.sum(col('dist_to')).alias('sum_dist'), \
                py.min(col('dist_to')).alias('min_dist')]

        
        #Measure the distance between first and last in each linear grouping and compare to sum distance of all points
        df_grp = self.df.groupBy('device_id', 'study_dt', 'lin_grp').agg(*expr) 
        df_l = self.df.filter(self.df['sequence'].isin([1,lgrp])).join(df_grp, ['device_id','study_dt','lin_grp'])


        # Only keep groups that meet criteria for being straight-line
        df_j = df_l.withColumn('strt_dist', distance(df_l['latitude'],df_l['longitude'], \
                    lead(df_l['latitude'],1).over(l), \
                    lead(df_l['longitude'],1).over(l), 'meters')) \
                .withColumn('lin', col('strt_dist') / df_l['sum_dist']) \
                .na.drop(subset=['strt_dist']) \
                .filter((df_l['min_dist'] > 0)  \
                    & (col('strt_dist').between(150, 2000)) \
                    & (df_l['cnt'] == 4) \
                    & (col('lin') >= .99825)) \
                .select('device_id','lin_grp', 'lin')

        # Outer join main dataframe to linears groups to filter non-linear pings 
        self.df = self.df.join(df_j, ['device_id','lin_grp'], how='left_outer') \
            .filter(col('lin').isNull()) \
            .drop('lin_grp', 'RecordNum', 'dist_to', 'sequence', 'lin')

        #lin_cnt = self.df.cache().count()
        lin_cnt = self.df.count()
        
        tbl_data = [['Initial count', init_cnt, 0, 0, 'Count of pings before applying linear movement filter'], \
           ['Final count', lin_cnt, init_cnt - lin_cnt, ((init_cnt - lin_cnt) / float(init_cnt)) * 100, \
            'Count of pings after applying linear movement filter']]

        # Display filter table
        print(tabulate(tbl_data, floatfmt=".2f", headers=['Phase', 'Ping Count', 'Removed Pings', \
                                                          'Percent Reduction', 'Description']))
        
        
    def write_results(self, base_path = "", partition_num = 100):
        print("\n_______________________________________________\nFINAL PING COUNT\n\n")
        self.df.groupBy(self.df['study_dt']).count().orderBy('study_dt').show()
        
        if base_path == "":
            pass
                
        else:
            for dt in self.study_dts:
                self.df \
                .filter(self.df['study_dt'] == str(dt)[0:10]) \
                .repartition(partition_num, 'device_id', 'study_dt').sortWithinPartitions('device_id','utc_timestamp') \
                .write \
                .csv(base_path + str(dt)[0:10].replace("-","") + "00", sep = ",", compression = "gzip")

class GpsPingCluster:
    def __init__(self):
        mpe = "2800M"
        pyspark_submit_args = ' --executor-memory ' + mpe + ' pyspark-shell'
        os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

    def init_spark(self, partition_num = 100, master_location = "spark://172.25.0.09:7077"):
        confg = pyspark.SparkConf()
        if master_location != "":            
            print("setting master to: ", master_location)
            confg.setMaster(master_location)
        self.context = pyspark.SparkContext(conf=confg)        
        self.spark = SQLContext(self.context)
        self.session = SparkSession.builder.appName('clean_and_cluster').getOrCreate()
        dataFrameReader = self.session.read
        self.spark.udf.register('anchor_dist',anchor_dist, FloatType())
        self.spark.udf.register("distance",distance,FloatType())
        self.part_num = partition_num
    
    def load_daily_feed_reduced(self, source='s3', start_dt=None, end_dt=None):
        '''
        Load daily-feed-reduced pings from specified dates.
        If dates left blank, program will automatically determine study date(s)
        "Source" options include 's3' or 'local'
        '''
        
        dfr_days = s3_dates('daily-feed-reduced')
        micro_days = s3_dates('micro-clusters')

        if start_dt is not None and end_dt is not None:
            study_dt = []
            diff = datetime.strptime(end_dt, '%Y-%m-%d') - datetime.strptime(start_dt, '%Y-%m-%d')
            for dt in range(diff.days+1):
                study_dt.append(datetime.strptime(start_dt, '%Y-%m-%d') + timedelta(days=dt))
            self.study_dts = sorted(study_dt)
        
        elif start_dt is not None and end_dt is None:
            self.study_dts = [datetime.strptime(start_dt, '%Y-%m-%d')]
                
        else:
            self.study_dts = sorted(list(set(dfr_days) - set(micro_days) - set([max(dfr_days)])))

        # Exit script if a study date is empty or date(s) for analysis don't exist  
        if len(set(micro_days) - set(dfr_days)) > 0:
            exit("Not all dates needed for analysis exist in the daily-feed-reduced")
        
        if len(self.study_dts) == 0:
            exit("There are no dates to process currently")

        # Create needed file paths (local or S3)
        if source == 's3' is True:
            base_path = 's3://b6-8f-fc-09-0f-db-50-3f-gpsdata/cuebiq/daily-feed-reduced/US/'
        else:
            base_path = "/opt/spark/sample_data/daily-feed-reduced/US/"
            
        dt_paths = []
        for dt in self.study_dts:
            dt_paths.append(base_path + str(dt).replace('-','')[0:8] + '*/*.csv.gz')
        print(dt_paths)

        df_schema = StructType([StructField("device_id",StringType(),True),
                    StructField("utc_timestamp",IntegerType(),True),
                    StructField("os",IntegerType(),True),
                    StructField("latitude",FloatType(),True),
                    StructField("longitude",FloatType(),True),
                    StructField("accuracy",IntegerType(),True),
                    StructField("tz_offset",IntegerType(),True),
                    StructField("study_dt",StringType(),True)])

        self.df = self.spark.read \
            .option("inferSchema", False) \
            .option("header", False) \
            .schema(df_schema) \
            .csv(",".join(dt_paths).split(','))
        
        print("\nRunning the following date(s) ({:,} total):".format(len(self.study_dts)))
        for dt in sorted(self.study_dts):
            print("   " + dt.strftime('%x'))
            
        print("\n{:,} pings loaded for analysis".format(self.df.count()))
        
    def chain_pings(self):
        print("\n_______________________________________________\nCHAINING PINGS\n\n")
        
        w = Window().partitionBy('device_id', 'study_dt').orderBy('utc_timestamp')
        init_cnt = self.df.count()
        
        self.df = self.df.withColumn('chain_dist', ((((self.df['accuracy'] + lead(self.df['accuracy'],1).over(w)) - 10) * (230 / 120) + 200))) \
                .withColumn('chain', when((distance(self.df['latitude'], self.df['longitude'], \
                           lead(self.df['latitude'],1).over(w), lead(self.df['longitude'], 1).over(w),'feet')) <= col('chain_dist'), 1) \
                          .when((distance(self.df['latitude'], self.df['longitude'], \
                         lag(self.df['latitude'],1).over(w), lag(self.df['longitude'], 1).over(w),'feet')) <= lag(col('chain_dist'), 1).over(w), 1).otherwise(0))
        
        self.unchain_df = self.df.filter(self.df['chain'] == 0) \
                                    .drop('chain_dist','chain')
        
        self.df = self.df.filter(self.df['chain'] == 1) \
                                    .drop('chain_dist','chain')
        
        unchain_cnt = self.unchain_df.cache().count()
        chain_cnt = self.df.cache().count()
        
        
        
        tbl_data = [['Initial count', init_cnt, 0, 0, 'Count of pings prior to analyzing spatial relationships'], \
                    ['Chained count', chain_cnt, init_cnt - chain_cnt, ((init_cnt - chain_cnt) / float(init_cnt)) * 100, \
                      'Count of pings that have spatially proximate neighbors to consider for clustering']]

        # Display filter table
        print(tabulate(tbl_data, floatfmt=".2f", headers=['Phase', 'Ping Count', 'Removed Pings', \
                                                          'Percent Reduction', 'Description']))
        
    def anchor_pings(self, dist_thresh = 430, time_thresh = 28800):
        '''
        anchor_dist = max threshold in feet to consider for anchoring
        time_thresh = max threshold in seconds to consider for clustering
        '''
        print("\n_______________________________________________\nANCHORING PINGS\n\n")
        print("{:,} pings being considered for anchoring\n".format(self.df.count()))
        
        w = Window().partitionBy('device_id', 'study_dt').orderBy('utc_timestamp')
        
        
        # Create empty anchor table
        schema = StructType([StructField("device_id",StringType(),True), 
                             StructField("utc_timestamp",IntegerType(),True),
                                StructField("os",IntegerType(),True),
                                StructField("latitude",FloatType(),True),
                                StructField("longitude",FloatType(),True),
                                StructField("accuracy",IntegerType(),True),
                                StructField("tz_offset",IntegerType(),True),
                                StructField("study_dt", StringType(),True),
                                StructField("row_number", IntegerType(),True),
                                StructField("anchor_test", ArrayType(FloatType()), True)])


        self.anchor_df = self.spark.createDataFrame(self.context.emptyRDD(),schema)
        self.orphan_df = self.spark.createDataFrame(self.context.emptyRDD(),schema).drop('anchor_test') 
        
        self.df = self.df.withColumn('row_number', row_number().over(w))        
        
        tot_anchor_cnt = 0
        tot_orphan_cnt = 0
        df_cnt = 1
        
        i = 0
        
        while df_cnt > 0:

            start_cnt = self.df.count()

            # Create initial anchor points
            self.df = self.df.withColumn('anchor', when(row_number().over(w) == 1, \
                                                        py.array(self.df['utc_timestamp'], self.df['latitude'], self.df['longitude'])) \
                                     .when(distance(self.df['latitude'], self.df['longitude'], \
                                            lag(self.df['latitude'],1).over(w),lag(self.df['longitude'],1).over(w),'feet') >= dist_thresh, \
                                                        py.array(self.df['utc_timestamp'], self.df['latitude'], self.df['longitude'])) \
                                      .when(self.df['utc_timestamp'] - lag(self.df['utc_timestamp'], 1).over(w) >= time_thresh, \
                                                        py.array(self.df['utc_timestamp'], self.df['latitude'], self.df['longitude']))) \
                        .withColumn('anchor_test', py.last('anchor', True).over(w)) \
                        .withColumn('test_dist', distance(self.df['latitude'], self.df['longitude'], col('anchor_test')[1], col('anchor_test')[2], 'feet')) \
                        .withColumn('thresh_flag', when(col('test_dist') > dist_thresh, 1)) \
                        .withColumn('cume_sum', py.sum(col('thresh_flag')).over(Window().partitionBy('device_id','study_dt','anchor_test') \
                                                                                .orderBy('utc_timestamp').rangeBetween(Window.unboundedPreceding, 0)))




            #self.df.show(500)

            self.anchor_df = self.anchor_df.union(self.df.filter(self.df['cume_sum'].isNull()) \
                                    .drop('anchor','test_dist','thresh_flag','cume_sum')) \
                                    #.repartition(self.part_num, 'device_id','study_dt')

            self.df = self.df.filter(self.df['cume_sum'].isNotNull()) \
                        .withColumn('orphan_test', when(self.df['row_number'] + 1 == lead(self.df['row_number'], 1).over(w), 1) \
                                                    .when(self.df['row_number'] - 1 == lag(self.df['row_number'], 1).over(w), 1))


            self.orphan_df = self.orphan_df.union(self.df.filter(self.df['orphan_test'].isNull()) \
                                                 .drop('anchor','test_dist','thresh_flag','cume_sum','orphan_test', 'anchor_test')) \
                                                #.repartition(self.part_num, 'device_id','study_dt')

            self.df = self.df.filter(self.df['orphan_test'].isNotNull()) \
                                .drop('anchor', 'anchor_test', 'test_dist', 'thresh_flag', 'cume_sum', 'orphan_test') \
                                .repartition(self.part_num, 'device_id','study_dt')


            #prev_anchor_cnt = tot_anchor_cnt
            #prev_orphan_cnt = tot_orphan_cnt
            #tot_anchor_cnt = self.anchor_df.cache().count()
            #tot_orphan_cnt = self.orphan_df.cache().count()
            #tot_anchor_cnt = self.anchor_df.count()
            #tot_orphan_cnt = self.orphan_df.count()


            #anchor_cnt = tot_anchor_cnt - prev_anchor_cnt
            #orphan_cnt = tot_orphan_cnt - prev_orphan_cnt
            df_cnt = self.df.cache().count()
            #df_cnt = self.df.count()

            i = i + 1
            print("Iteration {}: {:,} records remaining".format(i, df_cnt))
            
            #print("Iteration {}: {:,} anchored records identified. {:,} orphaned records identified. ({:,} records remaining)".format(i, anchor_cnt, orphan_cnt, df_cnt))
        
        
        #self.anchor_df = self.anchor_df(withColumn('anchor_key', anchor_df['anchor_test'][0]))
        #self.anchor_df.show(200)
        
        anchor_cnt = self.anchor_df.cache().repartition(self.part_num, 'device_id', 'study_dt').count()
        
        #test_agg = self.anchor_df.groupBy('device_id', 'anchor_test').count()
        #test_df = test_agg.groupBy(test_agg['count']).count()
        #test_df.show(100)
        
        self.anchor_df = self.anchor_df.withColumn('anchor_timestamp', self.anchor_df['anchor_test'][0].cast(IntegerType()))
        
        
        self.anchor_df.show(200)
        
        self.anchor_df.groupBy(self.anchor_df['device_id'], self.anchor_df['anchor_timestamp']).count().show(200)
        #test_agg2 = test_agg.groupBy('count').count().show(100)

        
        #tbl_data = [[i, tot_anchor_cnt, tot_orphan_cnt, self.df_anchor.groupBy(['anchor_test']   0, 'Count of pings prior to analyzing spatial relationships'], \
         #           ['Chained count', chain_cnt, init_cnt - chain_cnt, ((init_cnt - chain_cnt) / float(init_cnt)) * 100, \
         #             'Count of pings that have spatially proximate neighbors to consider for clustering']]

        # Display filter table
        #print(tabulate(tbl_data, floatfmt=".2f", headers=['Phase', 'Ping Count', 'Removed Pings', \
        #                                                  'Percent Reduction', 'Description']))