import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from src.analysis.tasks import MerchantAnalysis
from src.data.loader import DataLoader


class TestMerchantAnalysis:
    @pytest.fixture
    def cleaned_data(
        self,
        spark,
        sample_merchants_data,
        sample_transactions_data,
        sample_merchants_schema,
        sample_transactions_schema,
    ):
        """Get cleaned data for testing."""
        loader = DataLoader(spark)
        merchants_df = spark.createDataFrame(sample_merchants_data, sample_merchants_schema)
        transactions_df = spark.createDataFrame(
            sample_transactions_data, sample_transactions_schema
        )
        return loader.clean_data(transactions_df, merchants_df)

    def test_task1_top_merchants(self, cleaned_data):
        """Test top merchants by city and month."""
        analysis = MerchantAnalysis()
        result = analysis.task1_top_merchants_by_city_month(cleaned_data)

        # Should have results
        assert result.count() > 0

        # Check columns
        expected_columns = ["month", "city_id", "merchant_name", "purchase_total", "no_of_sales"]
        assert set(result.columns) == set(expected_columns)

        # Each city-month combination should have at most 5 merchants
        city_month_counts = result.groupBy("month", "city_id").count().collect()
        for row in city_month_counts:
            assert row["count"] <= 5

    def test_task2_average_sales(self, cleaned_data):
        """Test average sales by merchant and state."""
        analysis = MerchantAnalysis()
        result = analysis.task2_average_sale_by_merchant_state(cleaned_data)

        # Should have results
        assert result.count() > 0

        # Check columns
        expected_columns = ["merchant_name", "state_id", "average_amount"]
        assert set(result.columns) == set(expected_columns)

        # Average amounts should be positive
        for row in result.collect():
            assert row.average_amount > 0

    def test_task3_top_hours(self, cleaned_data):
        """Test top hours by category."""
        analysis = MerchantAnalysis()
        result = analysis.task3_top_hours_by_category(cleaned_data)

        # Should have results
        assert result.count() > 0

        # Check columns
        expected_columns = ["category", "hour"]
        assert set(result.columns) == set(expected_columns)

        # Each category should have at most 3 hours
        category_counts = result.groupBy("category").count().collect()
        for row in category_counts:
            assert row["count"] <= 3

    def test_task5_recommendations(self, cleaned_data):
        """Test business recommendations generation."""
        analysis = MerchantAnalysis()
        recommendations = analysis.task5_business_recommendations(cleaned_data)

        # Should return all expected keys
        expected_keys = {
            "top_cities",
            "top_categories",
            "monthly_trends",
            "hourly_patterns",
            "installment_recommendation",
        }
        assert set(recommendations.keys()) == expected_keys

        # Each recommendation should have data
        assert recommendations["top_cities"].count() > 0
        assert recommendations["top_categories"].count() > 0
        assert recommendations["monthly_trends"].count() > 0
        assert recommendations["hourly_patterns"].count() > 0
        assert recommendations["installment_recommendation"].count() > 0
