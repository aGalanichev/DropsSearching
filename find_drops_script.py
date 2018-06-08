#!/opt/cloudera/parcels/Anaconda-4.0.0/bin/python

import datetime
import pandas as pd
import argparse
from drop_finders import FingerprintFinder, HardwareFinder, PhoneFinder
from pandas import read_csv
import sys

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help='Choose feature for drops looking for(hardware_id/fingerprint/phone)', type=str)
    parser.add_argument("--start_date", help='date, where scanning begins. format=yyyymmdd', type=str)
    parser.add_argument("--end_date", help='date, where scanning ends. format=yyyymmdd', type=str)
    parser.add_argument("--drop_users_part", help='part of drops users in all users[0,1] (def. 0.1)', type=float)
    parser.add_argument("--max_users", help='max users in one day with same index', type=int)
    parser.add_argument("--drops_filename", help='name of file with drops (def. drops.csv)', type=str)
    
    args = parser.parse_args()
    
    drop_users_part = 0.1 if args.drop_users_part is None else args.drop_users_part
    max_users = 30 if args.max_users is None else args.max_users
    
    if args.mode not in {'hardware_id', 'fingerprint', 'phone'} or not 0 <= drop_users_part <= 1 or max_users < 0:
        print('Wrong arguments')
        sys.exit(-1)
    
    path = '/home/dandreev/Andreev/Anton_tmp/neighborhood_scripts'
    
    input_filename = args.drops_filename if args.drops_filename is not None else 'drops.csv'
    
    
    
    days = get_short_date_str_range(args.start_date, args.end_date)
    drops = read_csv('{}/input/{}'.format(path, input_filename), index_col=None).values[:,0].tolist()
    sc, sqlContext = get_sqlContext()
    sc.setLogLevel("OFF")
    
    finder = None
    if   args.mode == 'hardware_id': finder = HardwareFinder(drops, drop_users_part, max_users)
    elif args.mode == 'fingerprint': finder = FingerprintFinder(drops, drop_users_part, max_users)
    elif args.mode == 'phone': finder = PhoneFinder(drops, drop_users_part, max_users)
    
    results = finder.get_drops_in_range(sqlContext, days)
    
    for drops, day in results:
        filename = "{}/output/drops_{}_{}_{}.csv".format(path, args.mode, day, drop_users_part)
        drops.to_csv(filename, index=None)
    
    try: sc.stop()
    except: pass
    
    sys.exit(0)

def get_short_date_str_range(start_date, end_date = None):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) if end_date is not None else pd.to_datetime(start_date)
    
    days = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days+1)]
    days = [str(day).split(' ')[0].replace('-','') for day in days]
    return days

def get_sqlContext():
    import sys
    import os
        
    try: sc.stop()
    except: pass
    
    spark_home= '/opt/cloudera/parcels/CDH/lib/spark/'
    
    os.environ ['SPARK_HOME'] = spark_home
    
    sys.path.insert( 0, os.path.join( spark_home, 'python' ) )
    sys.path.insert( 0, os.path.join( spark_home, 'python/lib/py4j-0.9-src.zip' ) )
    
    from pyspark import SparkContext, SparkConf, HiveContext
    
    conf = SparkConf().setAppName( 'drops_finding' )\
                        .setMaster( 'yarn-client' )\
                        .setExecutorEnv('PATH', os.environ[ 'PATH' ] ) \
                        .set('spark.executor.core', '5' )\
                        .set('spark.executor.memory', '25g' )\
                        .set('spark.driver.core', '5' )\
                        .set('spark.driver.memory', '25g' )\
                        .set('spark.yarn.driver.memoryOverhead', '4096' ) \
                        .set('spark.yarn.executor.memoryOverhead', '4096' )\
                        .set('spark.kryoserializer.buffer.max', '2047')\
                        .set('spark.driver.maxResultSize', '8g')\
                        .set("spark.dynamicAllocation.enabled","true") \
                        .set("spark.dynamicAllocation.minExecutors","10") \
                        .set("spark.dynamicAllocation.maxExecutors","16") \
                        .set("spark.dynamicAllocation.initialExecutors","10") \
                        .set("spark.dynamicAllocation.executorIdleTimeout","60s") \
                        .set("spark.dynamicAllocation.schedulerBacklogTimeout","5s") \
                        .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout","5s") \
                        
    sc = SparkContext( conf=conf )
    sqlContext = HiveContext( sc )
    
    return sc, sqlContext

    
if __name__ == '__main__':
    main()