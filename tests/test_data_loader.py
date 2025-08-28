import path_helper  # This sets up the path

from src.data.loader import DataLoader


class TestDataLoader:
    def test_clean_data_merchant_name_handling(
        self,
        spark,
        sample_merchants_data,
        sample_transactions_data,
        sample_merchants_schema,
        sample_transactions_schema,
    ):
        """Test that merchant_id is used when merchant_name is null."""
        loader = DataLoader(spark)

        merchants_df = spark.createDataFrame(sample_merchants_data, sample_merchants_schema)
        transactions_df = spark.createDataFrame(
            sample_transactions_data, sample_transactions_schema
        )

        cleaned_df = loader.clean_data(transactions_df, merchants_df)

        # Check merchant without name (M004)
        m004_records = cleaned_df.filter(cleaned_df.merchant_id == "M004").collect()
        assert len(m004_records) == 1
        assert m004_records[0].merchant_name == "M004"

        # Check merchant not in merchants table (M005)
        m005_records = cleaned_df.filter(cleaned_df.merchant_id == "M005").collect()
        assert len(m005_records) == 1
        assert m005_records[0].merchant_name == "M005"

    def test_clean_data_category_handling(
        self,
        spark,
        sample_merchants_data,
        sample_transactions_data,
        sample_merchants_schema,
        sample_transactions_schema,
    ):
        """Test that null categories are replaced with 'Unknown category'."""
        loader = DataLoader(spark)

        merchants_df = spark.createDataFrame(sample_merchants_data, sample_merchants_schema)
        transactions_df = spark.createDataFrame(
            sample_transactions_data, sample_transactions_schema
        )

        cleaned_df = loader.clean_data(transactions_df, merchants_df)

        # Check null category handling
        null_category_records = cleaned_df.filter(cleaned_df.merchant_id == "M002").collect()
        assert len(null_category_records) == 1
        assert null_category_records[0].category == "Unknown category"

        # Check non-null categories remain unchanged
        electronics_records = cleaned_df.filter(cleaned_df.category == "Electronics").collect()
        assert len(electronics_records) == 2
