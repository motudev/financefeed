import pytest
from datetime import datetime
from transformations import clean_silver_news

def test_clean_silver_news_casts_types_correctly(spark):
    # 1. Arrange: Create fake Bronze data matching the Finnhub JSON
    mock_data = [{
        "category": "general", 
        "headline": "Databricks releases new features", 
        "datetime": 1700000000, 
        "source": "Finnhub", 
        # FIX: Provide an actual datetime so Spark can infer TimestampType
        "ingestion_time": datetime(2026, 9, 1, 12, 0, 0), 
        "unexpected_field": "Should be dropped"
    }]
    bronze_df = spark.createDataFrame(mock_data)
    
    # 2. Act: Run the pure transformation function
    silver_df = clean_silver_news(bronze_df)
    
    # 3. Assert: Verify the schema and transformations
    columns = silver_df.columns
    assert "unexpected_field" not in columns
    assert "headline" in columns
    
    # Verify the datetime was cast from long to timestamp
    dtypes = dict(silver_df.dtypes)
    assert dtypes["datetime"] == "timestamp"