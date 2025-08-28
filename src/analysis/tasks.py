"""Merchant transaction analysis tasks for Billups data.

This module contains the core business logic for analyzing merchant transaction data.
It provides methods to identify top-performing merchants, analyze sales patterns,
and generate business recommendations based on historical transaction data.

The analysis tasks include:
- Task 1: Top 5 merchants by purchase amount per city/month
- Task 2: Average sale amount per merchant per state
- Task 3: Top 3 hours for largest sales per category
- Task 4: Popular merchants and location-category correlation
- Task 5: Business recommendations for new merchants
"""

from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


class MerchantAnalysis:
    """
    Container for merchant data analysis tasks.

    This class encapsulates all analysis logic for the merchant transaction dataset.
    Each method corresponds to a specific business question and returns a DataFrame
    with the analysis results ready for visualization or further processing.

    All methods expect a cleaned DataFrame with the following columns:
    - merchant_id: Unique merchant identifier
    - merchant_name: Merchant business name
    - city_id: City identifier
    - state_id: State identifier
    - category: Product category
    - purchase_date: Transaction timestamp
    - purchase_amount: Transaction value
    - installments: Number of installment payments
    """

    def task1_top_merchants_by_city_month(self, df: DataFrame) -> DataFrame:
        """
        Generate top 5 merchants by purchase_amount for each month/city combination.

        This analysis helps identify the highest revenue-generating merchants in each
        city on a monthly basis, useful for:
        - Regional performance tracking
        - Identifying market leaders by location
        - Seasonal trend analysis by merchant

        Args:
            df: Cleaned transaction DataFrame with merchant and location data

        Returns:
            DataFrame with columns:
                - month: Formatted as 'MMM yyyy' (e.g., 'Jan 2023')
                - city_id: City identifier
                - merchant_name: Name of the merchant
                - purchase_total: Total purchase amount for the month
                  (rounded to 2 decimals)
                - no_of_sales: Number of transactions

        Note:
            Results are ranked by purchase_total (primary) and no_of_sales (secondary)
            to break ties. Only top 5 merchants per city/month are returned.
        """
        monthly_aggregated = df.groupBy(
            F.year("purchase_date").alias("year"),
            F.month("purchase_date").alias("month"),
            "city_id",
            "merchant_name",
        ).agg(F.sum("purchase_amount").alias("purchase_total"), F.count("*").alias("no_of_sales"))

        window_spec = Window.partitionBy("year", "month", "city_id").orderBy(
            F.desc("purchase_total"), F.desc("no_of_sales")
        )

        ranked_merchants = monthly_aggregated.withColumn(
            "rank", F.dense_rank().over(window_spec)
        ).filter(F.col("rank") <= 5)

        result = ranked_merchants.select(
            F.date_format(
                F.concat(
                    F.col("year").cast("string"),
                    F.lit("-"),
                    F.col("month").cast("string"),
                    F.lit("-01"),
                ),
                "MMM yyyy",
            ).alias("month"),
            "city_id",
            "merchant_name",
            F.round("purchase_total", 2).alias("purchase_total"),
            "no_of_sales",
        ).orderBy("year", "month", "city_id", "rank")

        return result

    def task2_average_sale_by_merchant_state(self, df: DataFrame) -> DataFrame:
        """
        Calculate average sale amount per merchant per state.

        This analysis reveals merchant pricing strategies and customer purchasing
        patterns across different states, helpful for:
        - Identifying premium vs budget merchants
        - Understanding regional price variations
        - Benchmarking merchant performance

        Args:
            df: Cleaned transaction DataFrame with merchant and state data

        Returns:
            DataFrame with columns:
                - merchant_name: Name of the merchant
                - state_id: State identifier
                - average_amount: Average purchase amount (rounded to 2 decimals)

        Note:
            Results are ordered by average_amount descending to highlight
            merchants with highest average transaction values.
        """
        avg_sales = df.groupBy("merchant_name", "state_id").agg(
            F.avg("purchase_amount").alias("average_amount")
        )

        result = avg_sales.select(
            "merchant_name", "state_id", F.round("average_amount", 2).alias("average_amount")
        ).orderBy(F.desc("average_amount"))

        return result

    def task3_top_hours_by_category(self, df: DataFrame) -> DataFrame:
        """
        Identify top 3 hours for largest sales per product category.

        This analysis uncovers peak shopping hours for different product categories,
        enabling:
        - Optimized staffing schedules
        - Targeted marketing campaigns
        - Inventory management by time of day

        Args:
            df: Cleaned transaction DataFrame with category and timestamp data

        Returns:
            DataFrame with columns:
                - category: Product category name
                - hour: Hour of day (0-23) when sales peak

        Note:
            Returns the top 3 hours for each category based on total sales volume.
            Hours are in 24-hour format (0=midnight, 23=11pm).
        """
        hourly_sales = df.groupBy("category", F.hour("purchase_date").alias("hour")).agg(
            F.sum("purchase_amount").alias("total_sales")
        )

        window_spec = Window.partitionBy("category").orderBy(F.desc("total_sales"))

        ranked_hours = hourly_sales.withColumn("rank", F.dense_rank().over(window_spec)).filter(
            F.col("rank") <= 3
        )

        result = ranked_hours.select(
            "category", F.col("hour").cast("string").alias("hour")
        ).orderBy("category", "rank")

        return result

    def task4_popular_merchants_location_analysis(
        self, df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """
        Analyze merchant popularity by location and dominant categories by city.

        This dual analysis provides insights into:
        1. Which merchants dominate each city's market
        2. What product categories are most popular in each city

        These insights help with:
        - Market entry strategies for new merchants
        - Understanding local consumer preferences
        - Competitive landscape analysis

        Args:
            df: Cleaned transaction DataFrame with merchant, city, and category data

        Returns:
            Tuple containing two DataFrames:

            1. top_merchants_by_city:
                - merchant_name: Name of the merchant
                - city_id: City identifier
                - transaction_count: Number of transactions
                - rank: Ranking within city (1-10)

            2. city_dominant_categories:
                - city_id: City identifier
                - dominant_category: Most popular category in the city
                - category_transactions: Transaction count for that category

        Note:
            Only top 10 merchants per city are returned to focus on market leaders.
        """
        merchant_popularity = df.groupBy("merchant_name", "city_id").agg(
            F.count("*").alias("transaction_count")
        )

        window_spec = Window.partitionBy("city_id").orderBy(F.desc("transaction_count"))

        top_merchants_by_city = merchant_popularity.withColumn(
            "rank", F.dense_rank().over(window_spec)
        ).filter(F.col("rank") <= 10)

        city_category_analysis = df.groupBy("city_id", "category").agg(
            F.count("*").alias("transaction_count"), F.sum("purchase_amount").alias("total_sales")
        )

        city_dominant_categories = (
            city_category_analysis.groupBy("city_id")
            .agg(F.max(F.struct("transaction_count", "category")).alias("max_struct"))
            .select(
                "city_id",
                F.col("max_struct.category").alias("dominant_category"),
                F.col("max_struct.transaction_count").alias("category_transactions"),
            )
        )

        return top_merchants_by_city, city_dominant_categories

    def task5_business_recommendations(self, df: DataFrame) -> dict[str, Any]:
        """
        Generate comprehensive business recommendations for new merchants.

        This analysis synthesizes historical transaction data to provide actionable
        insights for merchants entering the market, covering:
        - Geographic expansion opportunities
        - Product category selection
        - Seasonal planning
        - Operating hour optimization
        - Payment strategy (installments)

        Args:
            df: Cleaned transaction DataFrame with all required fields

        Returns:
            Dictionary containing DataFrames with recommendations:

            - 'top_cities': Top 5 cities by market opportunity
                Columns: city_id, total_sales, transaction_count, avg_transaction_value

            - 'top_categories': Top 5 product categories by performance
                Columns: category, total_sales, transaction_count, avg_transaction_value

            - 'monthly_trends': Sales patterns by month
                Columns: year, month, total_sales, transaction_count

            - 'hourly_patterns': Sales distribution by hour
                Columns: hour, total_sales, transaction_count

            - 'installment_recommendation': Profitability analysis of payment options
                Columns: installments, avg_purchase_amount, transaction_count,
                        gross_profit, expected_default_loss, net_profit,
                        profit_margin_pct
        """
        city_performance = (
            df.groupBy("city_id")
            .agg(
                F.sum("purchase_amount").alias("total_sales"),
                F.count("*").alias("transaction_count"),
                F.avg("purchase_amount").alias("avg_transaction_value"),
            )
            .orderBy(F.desc("total_sales"))
        )

        category_performance = (
            df.groupBy("category")
            .agg(
                F.sum("purchase_amount").alias("total_sales"),
                F.count("*").alias("transaction_count"),
                F.avg("purchase_amount").alias("avg_transaction_value"),
            )
            .orderBy(F.desc("total_sales"))
        )

        monthly_trends = (
            df.groupBy(
                F.year("purchase_date").alias("year"), F.month("purchase_date").alias("month")
            )
            .agg(
                F.sum("purchase_amount").alias("total_sales"),
                F.count("*").alias("transaction_count"),
            )
            .orderBy("year", "month")
        )

        hourly_patterns = (
            df.groupBy(F.hour("purchase_date").alias("hour"))
            .agg(
                F.sum("purchase_amount").alias("total_sales"),
                F.count("*").alias("transaction_count"),
            )
            .orderBy("hour")
        )

        installment_analysis = (
            df.groupBy("installments")
            .agg(
                F.avg("purchase_amount").alias("avg_purchase_amount"),
                F.count("*").alias("transaction_count"),
                F.sum("purchase_amount").alias("total_sales"),
            )
            .orderBy("installments")
        )

        installment_impact = self._analyze_installment_profitability(installment_analysis)

        return {
            "top_cities": city_performance.limit(5),
            "top_categories": category_performance.limit(5),
            "monthly_trends": monthly_trends,
            "hourly_patterns": hourly_patterns,
            "installment_recommendation": installment_impact,
        }

    def _analyze_installment_profitability(self, installment_df: DataFrame) -> DataFrame:
        """
        Analyze profitability of accepting installment payments.

        This method calculates the expected profitability of offering installment
        payment options, accounting for the risk of defaults. It helps merchants
        decide whether to offer installment plans and at what terms.

        Business assumptions:
        - 25% gross profit margin on all sales
        - 22.9% monthly default rate for installment payments
        - Defaulting customers pay 50% of total before defaulting
        - Installments are equal monthly payments

        Args:
            installment_df: DataFrame with installment aggregations containing:
                - installments: Number of installment payments (1 = full payment)
                - avg_purchase_amount: Average transaction value
                - transaction_count: Number of transactions
                - total_sales: Total sales volume

        Returns:
            DataFrame with profitability metrics:
                - installments: Payment term
                - avg_purchase_amount: Average transaction value
                - transaction_count: Number of transactions
                - gross_profit: Expected profit before defaults
                - expected_default_loss: Estimated loss from defaults
                - net_profit: Profit after accounting for defaults
                - profit_margin_pct: Net profit margin as percentage

        Note:
            Single payment transactions (installments=1) have 0% default rate.
            All monetary values are rounded to 2 decimal places.
        """
        analysis = (
            installment_df.withColumn("gross_profit", F.col("total_sales") * 0.25)
            .withColumn(
                "expected_default_loss",
                F.when(F.col("installments") > 1, F.col("total_sales") * 0.229 * 0.5).otherwise(0),
            )
            .withColumn("net_profit", F.col("gross_profit") - F.col("expected_default_loss"))
            .withColumn("profit_margin_pct", (F.col("net_profit") / F.col("total_sales")) * 100)
        )

        return analysis.select(
            "installments",
            F.round("avg_purchase_amount", 2).alias("avg_purchase_amount"),
            "transaction_count",
            F.round("gross_profit", 2).alias("gross_profit"),
            F.round("expected_default_loss", 2).alias("expected_default_loss"),
            F.round("net_profit", 2).alias("net_profit"),
            F.round("profit_margin_pct", 2).alias("profit_margin_pct"),
        )
