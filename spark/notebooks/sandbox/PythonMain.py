import pyspark
import argparse
from gps_spark_logic import GpsSparkLogic

# attempt to read input args from spark-submit job
parser = argparse.ArgumentParser()
parser.add_argument("--testfile", help="the full path and name to a test file")
args = parser.parse_args()

fileNameAndPath = "OOOOPS NOT SET";
if args.testfile:
    fileNameAndPath = args.testfile
    
gsl = GpsSparkLogic()
gsl.init_spark() #should default to whatever is in env on spark cluster....I think
#gsl.read_data()
#gsl.show_df_stats()
#gsl.save_test_s3_file("s3://69-72-69-73-iris/emr_test_output/")
gsl.save_test_s3_file()

