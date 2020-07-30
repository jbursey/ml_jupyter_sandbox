import pyspark
import argparse

# attempt to read input args from spark-submit job
parser = argparse.ArgumentParser()
parser.add_argument("--testfile", help="the full path and name to a test file")
args = parser.parse_args()

fileNameAndPath = "OOOOPS NOT SET";
if args.testfile:
    fileNameAndPath = args.testfile

# get spark context
confg = pyspark.SparkConf()
confg.setMaster("spark://172.25.0.09:7077")
context = pyspark.SparkContext(conf=confg)

# read file
contents = context.textFile("file:///" + fileNameAndPath)
print(contents)
print(contents.count())

lines = contents.filter(lambda line: "f" in line)
print(lines.count())
print(lines)


# do the things

