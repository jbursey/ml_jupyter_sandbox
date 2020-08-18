#%pyspark



from pyspark.sql import (SparkSession, DataFrameReader)
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType,
                               FloatType, IntegerType) 
from pyspark.sql.functions import (when, udf, col, lit, lead, lag, row_number)
from pyspark.sql import functions as py
from pyspark.sql import SQLContext
from datetime import (datetime, date, time, timedelta)
import math
import operator
import pygeohash as pgh
import pyspark
import boto3

s3 = boto3.resource('s3')

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



# FOR DRIVER ANALYSIS - CALCULATE DISTANCE BETWEEN 2 POINTS
def distance_dr(lat1, lon1, lat2, lon2, unit='miles'):
    units = {'miles':3963.19,'kilometers':6378.137,'meters':6378137,'feet':20902464}


    phi_1 = math.radians(lat1)
    phi_2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2-lon1)

    area = math.sin(delta_phi/2.0) ** 2 \
    + math.cos(phi_1) * math.cos(phi_2) * \
    math.sin(delta_lambda / 2.0) ** 2

    central_angle = 2 * math.asin((area ** 0.5))
    radius = units[unit.lower()]

    return abs(round((central_angle * radius),4))  


def rate_of_speed(distance, seconds, time_unit='hour'):
    '''
    Returns the rate of speed based on input distance and seconds
    '''
    time = {'second':1, 'minute':60, 'hour':3600, 'day':86400, 'year':31536000}
    return py.abs(distance / (seconds / time[time_unit]))

gps_schema = StructType([StructField("utc_timestamp",IntegerType(),True),
                        StructField("device_id",StringType(),True),
                        StructField("os",IntegerType(),True),
                        StructField("latitude",FloatType(),True),
                        StructField("longitude",FloatType(),True),
                        StructField("accuracy",IntegerType(),True),
                        StructField("tz_offset",IntegerType(),True)])


class DailyGpsReduce:
    def __init__ (self):
        return

    def init_context(self, master_location = "spark://172.25.0.09:7077"):
        confg = pyspark.SparkConf()
        confg.set("spark.sql.session.timeZone","UTC")
        if master_location != "":            
            print("setting master to: ", master_location)
            confg.setMaster(master_location)
        self.context = pyspark.SparkContext(conf=confg)        
        self.sqlContext = SQLContext(self.context)
        self.sqlContext.udf.register("distance",distance,FloatType())
        self.sqlContext.udf.register('rate_of_speed',rate_of_speed, FloatType())
        self.dataFrameReader = self.sqlContext.read
        return


class DailyGpsLogic:
    def __init__ (self):
        return
    def init_context(self, master_location = "spark://172.25.0.09:7077"):
        confg = pyspark.SparkConf()
        confg.set("spark.sql.session.timeZone","UTC")
        if master_location != "":            
            print("setting master to: ", master_location)
            confg.setMaster(master_location)
        self.context = pyspark.SparkContext(conf=confg)        
        self.sqlContext = SQLContext(self.context)
        #self.session = SparkSession.builder.appName('clean_and_cluster').getOrCreate()
        #self.sc = self.session.sparkContext
        #self.sqlContext = SQLContext(self.sc)
        #self.sqlContext.conf.set("spark.sql.session.timeZone","UTC")
        self.sqlContext.udf.register("distance",distance,FloatType())
        self.sqlContext.udf.register('rate_of_speed',rate_of_speed, FloatType())
        self.dataFrameReader = self.sqlContext.read
        self.geohash_udf_9 = py.udf(lambda y, x:pgh.encode(y, x, precision=9))
        return
    
    
    def test_read(self):
        self.df1 = self.dataFrameReader \
        .options(header = 'false', delimiter = '\t', codec = 'gzip') \
        .schema(gps_schema) \
        .format("csv") \
        .load("s3://b6-8f-fc-09-0f-db-50-3f-gpsdata/cuebiq/processed-data/US/micro-clusters/2020080500/part-00000-d66ee2c3-0470-420e-91e9-88f645d03f5f-c000.csv.gz")
        
        print(df1.count())

    def read_data(self, csvPathPattern = "/opt/spark/sample_data/daily-feed/US/*/*.csv.gz"):        
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
        .option("delimiter", '\t') \
        .option("sep", '\t') \
        .option("inferSchema", False) \
        .option("header", False) \
        .schema(schema) \
        .csv(csvPathPattern)
        
        print(self.df.count())


###########################################################################
# SPARK SESSION
###########################################################################
#session = SparkSession.builder.appName('clean_and_cluster').getOrCreate()
#dataFrameReader = session.read
#sc = session.sparkContext
#spark.conf.set("spark.sql.session.timeZone","UTC")


    ##############################################################################
    # USER DEFINED FUNCTIONS
    ##############################################################################
    def anchor_func(self, tot_iterations, checkpoints, threshold=1000000):
        '''
        Iterative function to anchor clusters together
        '''
        global df_dist
        global part_num
        global anchor_dist
        i = 1

            
        schema = StructType([StructField('tz_timestamp',IntegerType(), True),
                        StructField('device_id', StringType(), True),
                        StructField('os', IntegerType(), True),
                        StructField('latitude', FloatType(), True),
                        StructField('longitude', FloatType(), True),
                        StructField('accuracy', IntegerType(), True),
                        StructField('row_number', IntegerType(), True),
                        StructField('anchor', IntegerType(), True)
                        ])

        df_anchors = spark.createDataFrame(sc.emptyRDD(),schema)  

        while i <= tot_iterations:
            
            # Connect/create anchor points for current iteration
            df_dist = df_dist.withColumn('anchor2', when((df_dist['anchor'].isNull()) & (lag(df_dist['anchor'],1).over(w2).isNotNull()) & \
                                            (distance(df_dist['latitude'], df_dist['longitude'], \
                                                            lag(df_dist['latitude'],1).over(w2), lag(df_dist['longitude'],1).over(w2),'feet') \
                                                < anchor_dist), lag(df_dist['anchor'],1).over(w2))) \
                    .withColumn('anchor', when((lag(df_dist['anchor'],1).over(w2).isNotNull()) & \
                                                (distance(df_dist['latitude'], df_dist['longitude'], \
                                                            lag(df_dist['latitude'],1).over(w2), lag(df_dist['longitude'],1).over(w2),'feet') \
                                                    >= anchor_dist), df_dist['tz_timestamp']).otherwise(df_dist['anchor']))
    
            #Union new anchor points to anchor point table
            df_anchors = df_anchors.union(df_dist.filter(df_dist['anchor2'].isNotNull()) \
                        .select('tz_timestamp','device_id','os','latitude','longitude','accuracy', \
                            'row_number', df_dist['anchor2'].alias('anchor'))) 
                            #.repartition(part_num,'device_id')
        
            #Remove established anchor pings from main dataset
            df_dist = df_dist.filter(df_dist['anchor2'].isNull())
            
            # Calculate if anchor chains are finished based on post-filter ping patterns and add a flag value
            df_dist = df_dist.withColumn('filter', when(((df_dist['anchor'].isNotNull()) & \
                                                    (lead(df_dist['anchor'],1).over(w2).isNotNull())) | \
                                                ((df_dist['anchor'].isNotNull()) & (lead(df_dist['row_number'],1).over(w2).isNull())),1))
    
            # Union finished anchor chains to table of existing anchor rows    
            df_anchors = df_anchors.union(df_dist.filter(df_dist['filter'] == 1) \
                            .select('tz_timestamp','device_id','os','latitude','longitude','accuracy','row_number','anchor')) 
                            #.repartition(part_num, 'device_id')

            df_dist = df_dist.filter(df_dist['filter'].isNull()) \
                    .select('tz_timestamp','device_id','os','latitude','longitude','accuracy', \
                        'row_number','anchor')

            if (i > 0 and i % checkpoints == 0) or i == tot_iterations:

                df_dist = df_dist.localCheckpoint()
                dist_cnt = df_dist.count()

                df_anchors = df_anchors.repartition(part_num, 'device_id') \
                                .localCheckpoint()

                print('iteration = {} \ndf_dist count = {}\n' \
                    .format(i, dist_cnt))

                if dist_cnt < threshold:
                    break

            i += 1
        return df_anchors



    def study_dates(self, start_date=None, end_date=None):
        '''
        Returns the study date, as well as the study bounds for the Cubeiq folders and utc_timestamps
        Timestamps are offset to 4am
        '''
        epoch = datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')    

        print(start_date)
        if start_date == None:
            start_dt = datetime.combine(date.today(), time(4,0,0)) - timedelta(days=3)
        else:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(hours=4)
            
        if end_date == None:
            end_dt = start_dt
        else:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(hours=4)  

        day_diff = end_dt - start_dt

        study_dts = []
        for study_dt in range(day_diff.days+1):
            study_dts.append({'study_dt':start_dt + timedelta(days=study_dt), \
                                's3_before':str(start_dt + timedelta(days=study_dt-1)).replace('-','')[:8] + "00", \
                                's3_study_dt':str(start_dt + timedelta(days=study_dt)).replace('-','')[:8] + "00", \
                                's3_after':str(start_dt + timedelta(days=study_dt+1)).replace('-','')[:8] + "00", \
                                'utc_study_dt':int(((start_dt + timedelta(days=study_dt)) - epoch).total_seconds()), \
                                'utc_before':int(((start_dt + timedelta(days=study_dt-1)) - epoch).total_seconds()), \
                                'utc_after':int(((start_dt + timedelta(days=study_dt+1)) - epoch).total_seconds())})

        return study_dts


    def process_data(self, study_dt):
        ##############################################################################
        # DECLARE VARIABLES
        ##############################################################################
        
        dt_range = self.study_dates(study_dt)
        dt=dt_range
        s1_bucket_name = 'b6-8f-fc-09-0f-db-50-3f-gpsdata'
        s1_initial_bucket_depth = 'cuebiq/daily-feed/US/'
        s1_bucket_output = 'cuebiq/daily-feed-reduced/US/'
        s2_bucket_name = 'b6-8f-fc-09-0f-db-50-3f-gpsdata'
        s2_initial_bucket_depth = 'cuebiq/daily-feed-reduced/US/'
        s2_bucket_output = 'cuebiq/processed-data/US/micro-clusters/'
        anchor_dist = 430
        time_thresh = 28800
        part_num = 9

        gps_schema = StructType([StructField("utc_timestamp",IntegerType(),True),
                                StructField("device_id",StringType(),True),
                                StructField("os",IntegerType(),True),
                                StructField("latitude",FloatType(),True),
                                StructField("longitude",FloatType(),True),
                                StructField("accuracy",IntegerType(),True),
                                StructField("tz_offset",IntegerType(),True)])

        s2_gps_schema = StructType([StructField("utc_timestamp",IntegerType(),True),
                                StructField("device_id",StringType(),True),
                                StructField("os",IntegerType(),True),
                                StructField("latitude",FloatType(),True),
                                StructField("longitude",FloatType(),True),
                                StructField("accuracy",IntegerType(),True),
                                StructField("tz_offset",IntegerType(),True),
                                StructField("row_number",IntegerType(),True)])


        ##############################################################################
        # WINDOWS
        ##############################################################################
        w = Window().partitionBy('device_id').orderBy('utc_timestamp')
        l = Window().partitionBy('device_id','lin_grp').orderBy('utc_timestamp')
        w2 = Window().partitionBy('device_id').orderBy('row_number')

        ##############################################################################
        # BEGIN DAILY ITERATION
        ##############################################################################
    
        print("Reading in files for {}".format(str(dt['study_dt'])[:10]))
        print("s3://{}/{}[{}|{}|{}]/*.gz".format(s1_bucket_name, s1_initial_bucket_depth, dt['s3_before'], dt['s3_study_dt'], dt['s3_after']))
        print("")
        

        #################################################################################################
        # START STEP 1
        #################################################################################################
        df1 = dataFrameReader \
            .options(header = 'false', delimiter = '\t', codec = 'gzip') \
            .schema(gps_schema) \
            .format("csv") \
            .load("/opt/spark/sample_data/daily-feed/US/2020729*/*.csv.gz")
            #.load("s3://" + s1_bucket_name + "/" + s1_initial_bucket_depth +  dt['s3_before'] +"/*.gz") # the day before 

        df2 = dataFrameReader \
            .options(header = 'false', delimiter = '\t', codec = 'gzip') \
            .schema(gps_schema) \
            .format("csv") \
            .load("/opt/spark/sample_data/daily-feed/US/2020730*/*.csv.gz")
            #.load("s3://" + s1_bucket_name + "/" + s1_initial_bucket_depth +  dt['s3_study_dt'] +"/*.gz") # actual study date

        df3 = dataFrameReader \
            .options(header = 'false', delimiter = '\t', codec = 'gzip') \
            .schema(gps_schema) \
            .format("csv") \
            .load("/opt/spark/sample_data/daily-feed/US/2020731*/*.csv.gz")
            #.load("s3://" + s1_bucket_name + "/" + s1_initial_bucket_depth +  dt['s3_after'] +"/*.gz") # the day after

        # Union data from three inputs into 1 dataframe
        df = df1.union(df2).union(df3) \
            .repartition(part_num, 'device_id')

        del df1
        del df2
        del df3

        
        ##############################################################################
        # FILTER INITIAL JUNK RECORDS
        # Removes duplicated records (based on time and id), poor accuracy, bad coordinates, and timestamps outside of study range
        ##############################################################################
        df = df.na.drop(subset=['latitude','longitude','tz_offset','accuracy']) \
                    .filter(((df['accuracy'] >= 5) & (df['accuracy'] <= 65)) \
                            & ((~(df['latitude'] == 0)) | ~(df['longitude'] == 0)) \
                            & (df['utc_timestamp'] + df['tz_offset']) \
                                    .between(dt['utc_study_dt'], dt['utc_after'])) \
                    .dropDuplicates(['utc_timestamp','device_id']) 



        ##############################################################################
        # EXCESSIVE SPEED REMOVAL
        ##############################################################################
        df = df.withColumn('dist_to',distance(df['latitude'], df['longitude'], lead(df['latitude'],1).over(w), \
                            lead(df['longitude'],1).over(w))) \
            .withColumn('sec_to', (lead(df['utc_timestamp'], 1).over(w) - df['utc_timestamp'])) \
            .withColumn('speed_to', rate_of_speed(col('dist_to'), col('sec_to'),'hour')) \
            .withColumn('dist_from', lag(col('dist_to'), 1).over(w)) \
            .withColumn('sec_from', lag(col('sec_to'), 1).over(w)) \
            .withColumn('speed_from', lag(col('speed_to'), 1).over(w)) \
            .filter(((col('dist_to').isNull()) | (col('dist_from').isNull())) \
                        | ((((col('speed_from') + col('speed_to')) / 2) <= 90) | ((col('dist_to') >= 150) | (col('dist_from') >= 150))) \
                        & ((col('speed_from') < 600) & (col('speed_to') < 600)) \
                        & ((col('speed_from') < 20) | (col('speed_to') < 20))) \
            .select('utc_timestamp', 'device_id', 'os', 'latitude', 'longitude', 'accuracy', 'tz_offset')


        
        ##############################################################################
        # LINEAR TRAVEL PING REMOVAL
        # Break pings out into groups of 4 to measure the linear distance 
        ##############################################################################
        #Assign a record number and linear grouping and lead distance
        df = df.withColumn('RecordNum',row_number().over(w)) \
            .withColumn('lin_grp', py.ceil(row_number().over(w) / 4)) \
            .withColumn('dist_to', distance(df['latitude'], df['longitude'], \
                lead(df['latitude'],1).over(l), lead(df['longitude'],1).over(l),'meters')) 


        # Create aggregated table for linear groupings
        expr = [py.min(col('utc_timestamp')).alias('min_utc_timestamp'),py.max(col('utc_timestamp')).alias('max_utc_timestamp'), \
            py.count(col('utc_timestamp')).alias('cnt'),py.sum(col('dist_to')).alias('sum_dist'),py.min(col('dist_to')).alias('min_dist')]

        dfl_grp = df.groupBy('device_id','lin_grp').agg(*expr) 

        dfl_grp.createOrReplaceTempView('dfl_grp')
        df.createOrReplaceTempView('dfl')

        # Grab just the first and last records in each linear grouping and append aggregated info
        dfls = spark.sql("SELECT a.utc_timestamp, a.device_id, a.os, a.latitude, a.longitude, a.accuracy, a.tz_offset, \
                    a.lin_grp, b.sum_dist, b.min_dist, b.cnt \
                    FROM dfl as a INNER JOIN dfl_grp as b \
                    ON a.device_id = b.device_id \
                    AND a.lin_grp = b.lin_grp \
                    AND a.utc_timestamp = b.min_utc_timestamp \
                    UNION ALL \
                    SELECT a.utc_timestamp, a.device_id, a.os, a.latitude, a.longitude, a.accuracy, a.tz_offset, \
                    a.lin_grp, b.sum_dist, b.min_dist, b.cnt \
                    FROM dfl as a INNER JOIN dfl_grp as b \
                    ON a.device_id = b.device_id \
                    AND a.lin_grp = b.lin_grp \
                    AND a.utc_timestamp = b.max_utc_timestamp")
        
        # Measure the distance between first and last in each linear grouping and compare to sum distance of all points
        # Only keep groups that meet criteria for being straight-line
        df_j = dfls.withColumn('strt_dist', distance(dfls['latitude'],dfls['longitude'], \
                    lead(dfls['latitude'],1).over(l), \
                    lead(dfls['longitude'],1).over(l), 'meters')) \
                .withColumn('lin',col('strt_dist') / dfls['sum_dist']) \
                .na.drop(subset=['strt_dist']) \
                .filter((dfls['min_dist'] > 0)  \
                    & (col('strt_dist').between(150, 2000)) \
                    & (dfls['cnt'] == 4) \
                    & (col('lin') >= .99825)) \
                .select('device_id','lin_grp', 'lin')
        
        # Outer join main dataframe to linears groups to filter non-linear pings 
        df = df.join(df_j, ['device_id','lin_grp'], how='left_outer') \
            .filter(col('lin').isNull()) \
            .select('utc_timestamp','device_id', 'os', 'latitude', 'longitude', 'accuracy', 'tz_offset') 

        del dfl_grp
        del dfls
        del df_j
        


        #######################################
        # CHAIN
        # Calculating the dynamic chain threshold to find proximate ping relationships
        #######################################
        df = df.withColumn('chain_dist', ((((df['accuracy'] + lead(df['accuracy'],1).over(w)) - 10) * (230 / 120) + 200))) \
            .withColumn('chain', when((distance(df['latitude'], df['longitude'], \
                            lead(df['latitude'],1).over(w), lead(df['longitude'], 1).over(w),'feet')) <= col('chain_dist'), 1) 
                            .when((distance(df['latitude'], df['longitude'], \
                            lag(df['latitude'],1).over(w), lag(df['longitude'], 1).over(w),'feet')) <= lag(col('chain_dist'), 1).over(w), 1)) \
            .filter(col('chain') == 1) \
            .withColumn('row_number', row_number().over(w)) \
            .select('utc_timestamp','device_id', 'os', 'latitude', 'longitude', 'accuracy', 'tz_offset','row_number') \
            .persist()

        df \
            .repartition(100,'device_id').sortWithinPartitions('device_id','row_number') \
            .write \
            .csv(path="/opt/spark/sample_data/daily-feed-reduced/"+dt['s3_study_dt'], mode="append", compression="gzip", sep=",")
            #.csv(path="s3://" + s1_bucket_name + '/' + s1_bucket_output + dt['s3_study_dt'], mode="append", compression="gzip", sep=",")        


        ##############################################################################################
        # START STEP 2
        ##############################################################################################

        print('Begin micro-clustering')

        # INITIALIZE ANCHOR TABLE - Create initial anchor start points based on row number = 1 and distance threshold
        self.df_dist = df.withColumn('tz_timestamp', df['utc_timestamp'] + df['tz_offset']) \
                        .withColumn('anchor', when(df['row_number'] == 1, col('tz_timestamp')) \
                                .when(distance(df['latitude'], df['longitude'], \
                                                lag(df['latitude'],1).over(w2),lag(df['longitude'],1).over(w2),'feet') \
                                            >= anchor_dist, col('tz_timestamp')) \
                                .when(col('tz_timestamp') - lag(col('tz_timestamp'),1).over(w2) >= time_thresh, col('tz_timestamp'))) \
                        .select('tz_timestamp','device_id','os','latitude','longitude','accuracy','row_number','anchor') \
                        .repartition(part_num, 'device_id') \
                        .persist()

        print('df_dist starting count = {}'.format(self.df_dist.count())) # Materialize table for caching
        
        df.unpersist()
        del df
        

        #####################################################################################################
        # ITERATE THROUGH DATAFRAME ANCHOR PROCESS - iterations are broken out to speed up checkpointing
        # Checkpointing is used to chop off the physical plans of the dataframes that grow with each iteration
        ######################################################################################################
        df_anchor1 = self.anchor_func(3,3)
        df_anchor2 = self.anchor_func(5,5)
        df_anchor3 = self.anchor_func(12,6)
        df_anchor4 = self.anchor_func(20,5)
        df_anchor5 = self.anchor_func(30,5)
        df_anchor6 = self.anchor_func(50,5)
        df_anchor7 = self.anchor_func(80,5,1000000)
        df_anchor8 = self.anchor_func(1000,5,1000000)


        ##################################################################################################
        # Collect remaining pings to driver for Python analysis
        print('collect remaining pings')
        anchor_list = self.df_dist.rdd.map(lambda row: {'timestamp':row[0], 'device_id':row[1], 'latitude':row[3], \
                                                'longitude':row[4], 'anchor':row[7]}).collect()

        # Sort elements in list by device_id and timestamp
        anchor_list.sort(key = operator.itemgetter('device_id','timestamp'))

        # Python analysis on driver of final remaining pings
        print('iterate through remaining pings on driver')
        anchor_dr = []

        for r in anchor_list:
            if r['anchor'] is not None:
                anchor_dr.append(r)
                
            else:
                if anchor_dr[-1]['device_id'] == r['device_id']:
                    if distance_dr(r['latitude'],r['longitude'], \
                                anchor_dr[-1]['latitude'], \
                                anchor_dr[-1]['longitude'], 'feet') <= anchor_dist \
                                & r['timestamp'] - anchor_dr[-1]['timestamp'] < time_thresh:
                        anchor_dr.append({'timestamp':r['timestamp'], 'device_id':r['device_id'], \
                                        'latitude':anchor_dr[-1]['latitude'], 'longitude':anchor_dr[-1]['longitude'], \
                                        'anchor':anchor_dr[-1]['anchor']})
                    
                    else:
                        r['anchor'] = r['timestamp']
                        anchor_dr.append(r)
                        
        # Condense result table for dataframe distribution
        print('generate driver anchor table')
        new_anchor = []
        for r in anchor_dr:
            new_anchor.append([r['timestamp'], r['device_id'], r['anchor']])

        # Bring driver results back into a distributed dataframe and join results
        print('disperse driver anchor table back to cluster')
        new_anchor_schema = StructType([StructField('tz_timestamp',IntegerType(), True),
                            StructField('device_id', StringType(), True),
                            StructField('anchor', IntegerType(), True)])

        df_anchor_dr = spark.createDataFrame(new_anchor,new_anchor_schema) \
                        .repartition(part_num, 'device_id')

        # Join remaining anchors to main analysis table
        self.df_dist = self.df_dist.select('tz_timestamp','device_id','os','latitude','longitude', \
                                'accuracy','row_number') \
                            .join(df_anchor_dr,['tz_timestamp','device_id']) \

        # Union all anchor tables together and sort
        print('finalizing anchor results into central table')
        df_anchors_fnl = df_anchor1.union(df_anchor2).union(df_anchor3).union(df_anchor4).union(df_anchor5) \
                            .union(df_anchor6).union(df_anchor7).union(df_anchor8).union(self.df_dist) \
                            .repartition(part_num,'device_id') \
                            .persist()
                            
        self.df_dist.unpersist()


        #######################################################################################
        # Calculate centroids
        #######################################################################################
        print('start calculating centroids')    
        # Get max accuracy value for each micro-cluster and filter clusters with fewer than 2 pings
        df_anchor_grp = df_anchors_fnl.groupBy('device_id','anchor').agg(*[py.max(col('accuracy')).alias('max_accuracy'), \
                                                                        py.count(col('tz_timestamp')).alias('cnt')]) \
                                    .withColumn('max_acc_1', col('max_accuracy') + 1) \
                                    .filter(col('cnt') > 1) \
                                    .select('device_id','anchor','max_acc_1','cnt')


        # Calculate the nominator for each micro-cluster
        df_anchors_fnl = df_anchors_fnl.join(df_anchor_grp, ['device_id','anchor']) \
                                        .withColumn('nom',col('max_acc_1') - col('accuracy'))


        df_denom = df_anchors_fnl.groupBy('device_id','anchor').agg(*[py.sum(col('nom')).alias('denom')])


        df_anchors_fnl = df_anchors_fnl.join(df_denom, ['device_id','anchor']) \
                            .withColumn('weight', df_anchors_fnl['nom'] / df_denom['denom']) \
                            .withColumn('lat', df_anchors_fnl['latitude'] * col('weight')) \
                            .withColumn('lon', df_anchors_fnl['longitude'] * col('weight'))


        expr = [py.sum(col('lat')).alias('new_latitude'), py.sum(col('lon')).alias('new_longitude'), \
                    py.avg(col('latitude')).alias('avg_latitude'), py.avg(col('longitude')).alias('avg_longitude'), \
                    py.count(col('tz_timestamp')).alias('cluster_png_cnt'), py.first(col('os')).alias('os'), \
                    py.min(col('tz_timestamp')).alias('start_timestamp'), py.max(col('tz_timestamp')).alias('end_timestamp'), \
                    py.avg(col('accuracy')).alias('avg_accuracy')]

        df_micro = df_anchors_fnl.groupBy('device_id','anchor').agg(*expr) \
                                .withColumn('fnl_lat', (col('new_latitude') * (3/4)) + (col('avg_latitude') * (1/4))) \
                                .withColumn('fnl_lon', (col('new_longitude') * (3/4)) + (col('avg_longitude') * (1/4))) \
                                .withColumn('geohash9', geohash_udf_9(col('fnl_lat'), col('fnl_lon'))) \
                                .withColumn('dwell_seconds', col('end_timestamp') - col('start_timestamp')) \
                                .withColumn('start_tm', py.from_unixtime(col('start_timestamp'))) \
                                .withColumn('end_tm', py.from_unixtime(col('end_timestamp'))) \
                                .filter(col('dwell_seconds') > 1) \
                                .select('device_id','os','start_tm','end_tm', \
                                        'dwell_seconds','cluster_png_cnt', col('fnl_lat').alias('latitude'), \
                                        col('fnl_lon').alias('longitude'), 'geohash9', 'avg_accuracy')


        df_micro \
                .repartition(100,'device_id').sortWithinPartitions('device_id','start_tm') \
                .write \
                .csv(path="/opt/spark/sample_data/processed-data/" + dt['s3_study_dt'], mode="append", compression="gzip", sep=",")
                #.csv(path="s3://" + s2_bucket_name + '/' + s2_bucket_output + dt['s3_study_dt'], mode="append", compression="gzip", sep=",")


        df_anchors_fnl.unpersist()

        return
    



