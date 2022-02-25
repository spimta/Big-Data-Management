from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
import json
import numpy as np
import sys


def expandVisits(date_range_start, visits_by_day):
    date = date_range_start.split('T')[0].split('-')
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    vbd = visits_by_day[1:-1].split(',')
    row = []

    week_day_count = 0
    for i in vbd:
      date2 = datetime.date(year, month, day) + datetime.timedelta(days=week_day_count)
      if year == 2019 or year == 2020:
        row.append((year, date2.strftime("%m-%d"), int(i)))
      week_day_count += 1

    return row


# Remember to use groupCount to know how long the visits list should be
def computeStats(group, visits):
    np_visits = np.array(visits)
    np_visits.resize(groupCount[int(group)])

    median = np.median(np_visits)
    stddev = np.std(np_visits)
    low = median - stddev
    low = low if low > 0 else 0
    high = median + stddev

    return (int(median), int(low), int(high))


def main(sc, spark):
    global groupCount

    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    dfPlaces = spark.read.csv('/data/share/bdm/core-places-nyc.csv', header=True, escape='"')
    dfPattern = spark.read.csv('/data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')

    OUTPUT_PREFIX = sys.argv[1]

    CAT_CODES = {'445210', '722515', '445299', '445120', '452210', '311811', '722410', '722511', '445220', '445292',
                 '445110', '445291', '445230', '446191', '446110', '722513', '452311'}
    CAT_GROUP = {'452311': 0, '452210': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446191': 5, '446110': 5,
                 '722515': 6, '311811': 6, '445299': 7, '445220': 7, '445292': 7, '445291': 7, '445230': 7, '445210': 7,
                 '445110': 8}

    udfToGroup = F.udf(lambda x: CAT_GROUP.get(x))

    dfPlaces = dfPlaces. \
        select('placekey', 'naics_code'). \
        filter(dfPlaces.naics_code.isin(CAT_CODES)). \
        withColumn('group', udfToGroup('naics_code')). \
        drop('naics_code').cache()

    groupCount = dfPlaces.groupBy(dfPlaces.group).count().rdd.map(lambda x: (int(x[0]), x[1])).collectAsMap()

    visitType = T.StructType([T.StructField('year', T.IntegerType()),
                              T.StructField('date', T.StringType()),
                              T.StructField('visits', T.IntegerType())])

    udfExpand = F.udf(expandVisits, T.ArrayType(visitType))

    dfPattern = dfPattern.join(dfPlaces, 'placekey') \
        .withColumn('expanded', F.explode(udfExpand('date_range_start', 'visits_by_day'))) \
        .select('group', 'expanded.*').cache()

    statsType = T.StructType([T.StructField('median', T.IntegerType()),
                              T.StructField('low', T.IntegerType()),
                              T.StructField('high', T.IntegerType())])

    udfComputeStats = F.udf(computeStats, statsType)

    df = dfPattern.groupBy('group', 'year', 'date') \
        .agg(F.collect_list('visits').alias('visits')) \
        .withColumn('stats', udfComputeStats('group', 'visits')) \
        .select('group', 'year', 'date', 'stats.*') \
        .withColumn('date', F.concat(F.lit('2020-'), dfPattern.date)) \
        .cache()

    GROUP = {"big_box_grocers": 0,
             "convenience_stores": 1,
             "drinking_places": 2,
             "full_service_restaurants": 3,
             "limited_service_restaurants": 4,
             "pharmacies_and_drug_stores": 5,
             "snack_and_bakeries": 6,
             "specialty_food_stores": 7,
             "supermarkets_except_convenience_stores": 8}

    for filename, group_num in GROUP.items():
        df.filter(df.group == group_num) \
            .drop('group') \
            .orderBy('year', 'date') \
            .cache() \
            .coalesce(50).write.csv(f'{OUTPUT_PREFIX}/{filename}', mode='overwrite', header=True)


if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    main(sc, spark)