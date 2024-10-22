from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, avg, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F

from datetime import datetime

hdfs_path = 'hdfs://wakuwa:9000/recruitment/silver/'
year, month, day = datetime.now().year, datetime.now().month, datetime.now().day

def write_to_hive(df, database_name, table_name):
    try:
        df = df.withColumn('year', F.lit(year)) \
            .withColumn('month', F.lit(month)) \
            .withColumn('day', F.lit(day))
        
        df.write \
            .format('hive') \
            .mode('overwrite') \
            .partitionBy("year", "month", "day") \
            .option("partitionOverwriteMode", "dynamic") \
            .saveAsTable(database_name + '.' + table_name)
        
        print('processed')
    except Exception as e:
        print(e)
if __name__ == '__main__':
    sc = SparkSession.builder \
        .appName("recruitmentProjectGoldLayer") \
        .config('spark.hadoop.hive.metastore.uris', "thrift://wakuwa:9083") \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    df = sc.read.options(
        header=True, 
        delimiter=',', 
        multiline=True,
        timestampFormat='yyyy-MM-dd HH:mm:ss',
        inferSchema=True,
        escape='"',
        quote='"'
    ).csv(hdfs_path)

    df = df.drop('year', 'month', 'day', 'source', 'jobCategory', 'jobDecription')

    df = df.withColumn("max_salary", col("max_salary").cast("integer")) \
            .withColumn("min_salary", col("min_salary").cast("integer")) \
            .withColumn('skills', F.from_json('Skills', ArrayType(StringType()))) \
            .withColumn('requirement', F.from_json('Requirement', ArrayType(StringType()))) \
            .withColumn('category', F.from_json('Category', ArrayType(StringType()))) \
            .withColumn('created_time', to_timestamp(col('Created_time'), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('last_updated', to_timestamp(col('Last_updated'), 'yyyy-MM-dd HH:mm:ss'))

    # for optimizing
    df.cache()
    # Validate data
    lower_cols = [col.lower() for col in df.columns]
    df = df.toDF(*lower_cols)

    # write_to_hive(df, "recruitment", "recruitment_all_report")
    
    # category - salary report
    category_exploded = df.withColumn('category', explode(col('category')))

    categories = category_exploded.groupBy('category').agg(
        ((F.avg(col('min_salary')) + F.avg(col('max_salary'))) / 2).alias('avg_salary'),
        F.count(col('Title')).alias('number_of_jobs')
    ).orderBy(
        col('number_of_jobs').desc(), 
        col('avg_salary').desc()
    )

    # write_to_hive(categories, "recruitment", "category_salary_report")

    #requirement - salary
    requirement_exploded = df.withColumn('requirement', explode(col('requirement')))
    requirement_exploded = requirement_exploded.where(~(requirement_exploded['requirement'].rlike('tuổi')))

    requirements = requirement_exploded.groupBy('requirement').agg(
        ((F.avg(col('min_salary')) + F.avg(col('max_salary'))) / 2).alias('avg_salary'),
        F.count(col('Title')).alias('number_of_jobs')
    ).orderBy(
        col('number_of_jobs').desc(), 
        col('avg_salary').desc()
    )
    
    write_to_hive(requirements, "recruitment", "requirement_salary_report")