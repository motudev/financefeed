from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def clean_silver_news(df: DataFrame) -> DataFrame:
    """Extracts core fields and casts data types from the raw JSON payload."""
    return df.select(
        col("category").cast("string"),
        col("headline").cast("string"),
        # Finnhub provides a UNIX timestamp
        col("datetime").cast("timestamp"), 
        col("source").cast("string"),
        col("ingestion_time")
    )

def aggregate_gold_news(df: DataFrame) -> DataFrame:
    """Aggregates article counts by source."""
    return df.groupBy("source").count().withColumnRenamed("count", "article_count")

