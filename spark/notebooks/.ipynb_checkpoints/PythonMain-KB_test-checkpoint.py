import pyspark
import argparse
from new_gps_spark_logic import GpsPingFilter

# attempt to read input args from spark-submit job
parser = argparse.ArgumentParser()
parser.add_argument("--testfile", help="the full path and name to a test file")
args = parser.parse_args()

#fileNameAndPath = "OOOOPS NOT SET";
#if args.testfile:
#    fileNameAndPath = args.testfile
    

gpf = GpsPingFilter()

gpf.init_spark("")

gpf.load_daily_feed('s3','2020-08-09')

gpf.junk_filter()

gpf.speed_filter()

gpf.linear_filter()

gpf.write_results()