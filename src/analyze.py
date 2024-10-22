from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F

if __name__ == "__main__":
    sc = SparkSession.builder \
        .appName('Transformation') \
        .hiveEnabled(True) \
        .getOrCreate()

    path = '/recruitment/silver'

    df = sc.read.options(header=True, 
                        delimiter=',',
                        multiline=True,
                        escape='"')\
            .csv(path)

    df = df.drop('year', 'month', 'day', 'source', 'jobDescription')
    df.show(5)

    df = df.withColumn("max_salary", col("max_salary").cast("integer")) \
        .withColumn("min_salary", col("min_salary").cast("integer")) \
        .withColumn('Skills', F.from_json('Skills', ArrayType(StringType()))) \
        .withColumn('Requirement', F.from_json('Requirement', ArrayType(StringType()))) \
        .withColumn('Category', F.from_json('Category', ArrayType(StringType()))) \
        .withColumn('Created_time', to_timestamp(col('Created_time'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('Last_updated', to_timestamp(col('Last_updated'), 'yyyy-MM-dd HH:mm:ss'))
    
    # Explode 'Skills' column and select all columns from original DataFrame + exploded 'Skills'
    exploded_skill = df.withColumn("Skills", explode(df['Skills']))

    exploded_skill.show(5)

    # Pivoting salary_range and grouping by Skills
    pivot_df = exploded_skill.groupBy('Skills') \
        .pivot('salary_range') \
        .agg(F.count('Title')) \
        .fillna(0)  # Replace nulls with 0 for clarity

    # Add a 'total' column to sum across all salary ranges
    pivot_df = pivot_df.withColumn('total', 
        sum(pivot_df[col] for col in pivot_df.columns if col != 'Skills')
    )

    # Write into hive
    pivot_df.write.mode('overwrite').saveAsTable('skills_salary_report')