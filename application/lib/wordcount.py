#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse
import logging

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


def create_context(appName):
    """
    Creates Spark HiveContext
    """
    logger.info("Creating Spark context - may take some while")

    spark = SparkSession.builder \
            .appName(appName) \
            .config("spark.hadoop.validateOutputSpecs", "false") \
            .enableHiveSupport() \
            .getOrCreate()
    return spark


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """

    parser = optparse.OptionParser(description='PySpark WordCount.')
    parser.add_option('-i', '--input', action='store', nargs=1,
                      default='s3://dimajix-training/data/alice/',
                      help='Input file or directory')
    parser.add_option('-o', '--output', action='store', nargs=1,
                      default='alice-counts',
                      help='Output file or directory')

    (opts, args) = parser.parse_args()

    return opts


def run():
    opts = parse_options()

    logger.info("Creating Spark Context")
    spark = create_context(appName="WordCount")

    logger.info("Starting processing")

    # 1. Read text file, one record per line
    text = spark.read.text(opts.input)
    # 2. Using the split function, split each record into a list of words
    word_lists = text.select(split(text.value, ' ').alias("word_list"))
    # 3. Using the explode function, convert each list into individual records
    words = word_lists.select(explode(word_lists.word_list).alias("word"))
    # 4. Remove empty words
    non_empty_words = words.filter(words.word != '')
    # 5. Group by word and count frequency
    result = non_empty_words.groupBy(words.word).count()
    # 6. Sort words by frequency (descending)
    sorted_result = result.orderBy(result['count'].desc())
    # 7. Save result
    sorted_result.write.csv(opts.output)

    logger.info("Successfully finished processing")
    spark.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('').setLevel(logging.INFO)

    logger.info("Starting main")
    run()
    logger.info("Successfully finished main")


if __name__ == "__main__":
    main()

