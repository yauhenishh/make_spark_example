#!/usr/bin/env python
"""
Main Spark Job Entry Point for Billups Merchant Data Analysis.

This module provides a unified entry point for running all merchant data
analysis tasks using Apache Spark. It supports both local and distributed
execution modes, with optional Hive integration for data persistence.

The job analyzes historical transaction data to provide insights including:
- Top performing merchants by location and time period
- Average sales metrics by merchant and geography
- Peak business hours by product category
- Location-based merchant popularity analysis
- Business recommendations for new merchants

Example:
    Run all analysis tasks locally:
        $ python src/spark_job.py -t data/transactions.parquet \
            -m data/merchants.csv --task all

    Run a specific task with Hive integration:
        $ python src/spark_job.py -t data/transactions.parquet -m data/merchants.csv --task 1

    Submit to Spark cluster:
        $ spark-submit --master spark://master:7077 src/spark_job.py \\
            -t hdfs://data/transactions.parquet -m hdfs://data/merchants.csv --task all
"""

import argparse
import os
import sys
from pathlib import Path

# Add the parent directory to the Python path to enable imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.analysis.tasks import MerchantAnalysis
from src.data.loader import DataLoader
from src.utils.hive_utils import get_spark_with_hive
from src.utils.spark_utils import create_spark_session, save_results


class SparkJob:
    """
    Main Spark job class for orchestrating merchant data analysis tasks.

    This class manages the Spark session lifecycle, data loading, and execution
    of various analysis tasks. It provides methods to run individual tasks or
    all tasks in sequence, with support for both file-based and Hive-based output.

    Attributes:
        transactions_path (str): Path to the historical transactions parquet file
        merchants_path (str): Path to the merchants CSV file
        use_hive (bool): Whether to use Hive for data persistence
        spark (SparkSession): Active Spark session
        loader (DataLoader): Data loader instance for reading and cleaning data
        analysis (MerchantAnalysis): Analysis instance containing task implementations
        _cleaned_data (DataFrame): Cached cleaned dataset to avoid reprocessing
    """

    def __init__(
        self,
        transactions_path: str,
        merchants_path: str,
        use_hive: bool = True,
        spark_config: dict[str, str] | None = None,
    ):
        """
        Initialize the Spark job with data paths and configuration.

        Args:
            transactions_path: Path to the historical transactions parquet file.
                Can be a local path or HDFS path (hdfs://...)
            merchants_path: Path to the merchants CSV file.
                Can be a local path or HDFS path (hdfs://...)
            use_hive: Whether to enable Hive support for data persistence.
                When True, results will be written to Hive tables in addition
                to output files.
            spark_config: Optional dictionary of Spark configuration parameters.
                Common configs include spark.executor.memory, spark.executor.cores, etc.

        Raises:
            RuntimeError: If Spark session creation fails
        """
        self.transactions_path = transactions_path
        self.merchants_path = merchants_path
        self.use_hive = use_hive

        if use_hive:
            self.spark = get_spark_with_hive()
        else:
            if spark_config:
                builder = SparkSession.builder.appName("BillupsDataAnalysis")
                for key, value in spark_config.items():
                    builder = builder.config(key, value)
                self.spark = builder.getOrCreate()
            else:
                self.spark = create_spark_session()

        self.loader = DataLoader(self.spark, use_hive=use_hive)
        self.analysis = MerchantAnalysis()
        self._cleaned_data = None

    def get_cleaned_data(self):
        """
        Get the cleaned and joined dataset, using cache if available.

        This method loads transaction and merchant data, performs cleaning operations
        (filling nulls, joining datasets), and caches the result for subsequent use.
        The caching prevents redundant data loading and processing when running
        multiple analysis tasks.

        Returns:
            DataFrame: Cleaned dataset with transactions and merchant
                information joined. Contains columns: merchant_id,
                merchant_name, city_id, state_id, category, purchase_date,
                purchase_amount, installments
        """
        if self._cleaned_data is None:
            print("\n" + "=" * 60)
            print("LOADING AND CLEANING DATA")
            print("=" * 60)

            self._cleaned_data = self.loader.get_cleaned_data(
                self.transactions_path, self.merchants_path
            )

            # Cache the data for better performance
            self._cleaned_data.cache()

            print("\nData Schema:")
            self._cleaned_data.printSchema()

            total_records = self._cleaned_data.count()
            print(f"\nTotal records loaded: {total_records:,}")

            # Show basic statistics
            print("\nBasic Statistics:")
            unique_merchants = self._cleaned_data.select("merchant_id").distinct().count()
            unique_cities = self._cleaned_data.select("city_id").distinct().count()
            unique_states = self._cleaned_data.select("state_id").distinct().count()
            unique_categories = self._cleaned_data.select("category").distinct().count()

            print(f"Unique merchants: {unique_merchants:,}")
            print(f"Unique cities: {unique_cities:,}")
            print(f"Unique states: {unique_states:,}")
            print(f"Unique categories: {unique_categories:,}")

            # Show sample data
            print("\nSample data (first 5 rows):")
            self._cleaned_data.show(5, truncate=False)

        return self._cleaned_data

    def run_task1(self, output_path: str | None = None) -> DataFrame:
        """
        Execute Task 1: Identify top 5 merchants by purchase amount for each month/city.

        This task aggregates transaction data to find the highest performing merchants
        in each city for each month. Results include total purchase amount and
        transaction count per merchant.

        Args:
            output_path: Optional path to save results as CSV. If not provided,
                results are only displayed and/or saved to Hive.

        Returns:
            DataFrame: Results containing columns:
                - month: Formatted as 'MMM yyyy' (e.g., 'Jan 2023')
                - city_id: City identifier
                - merchant_name: Name of the merchant
                - purchase_total: Total purchase amount for the month
                - no_of_sales: Number of transactions
        """
        print("\n" + "=" * 60)
        print("TASK 1: TOP 5 MERCHANTS BY CITY AND MONTH")
        print("=" * 60)

        df = self.get_cleaned_data()

        print("\nAnalyzing merchant performance by city and month...")
        result = self.analysis.task1_top_merchants_by_city_month(df)

        print("\nResult Schema:")
        result.printSchema()

        total_results = result.count()
        print(f"\nTotal result records: {total_results:,}")

        # Show insights
        print("\nKey Insights:")
        unique_months = result.select("month").distinct().count()
        unique_cities = result.select("city_id").distinct().count()
        print(f"- Analysis covers {unique_months} months across {unique_cities} cities")
        print("- Each city-month combination shows top 5 merchants")
        print("- Rankings based on total purchase amount with sales count as tiebreaker")

        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            save_results(result, output_path, format="csv")
            print(f"\n✓ Results saved to {output_path}")

        print("\nTop Merchants Sample (first 20 rows):")
        result.show(20, truncate=False)

        return result

    def run_task2(self, output_path: str | None = None) -> DataFrame:
        """
        Execute Task 2: Calculate average sale amount per merchant per state.

        This task computes the average purchase amount for each merchant across
        different states, helping identify merchant performance patterns by geography.

        Args:
            output_path: Optional path to save results as CSV. If not provided,
                results are only displayed and/or saved to Hive.

        Returns:
            DataFrame: Results containing columns:
                - merchant_name: Name of the merchant
                - state_id: State identifier
                - average_amount: Average purchase amount rounded to 2 decimal places
        """
        print("\n" + "=" * 60)
        print("TASK 2: AVERAGE SALE BY MERCHANT AND STATE")
        print("=" * 60)

        df = self.get_cleaned_data()

        print("\nCalculating average transaction values...")
        result = self.analysis.task2_average_sale_by_merchant_state(df)

        print("\nResult Schema:")
        result.printSchema()

        total_results = result.count()
        print(f"\nTotal merchant-state combinations: {total_results:,}")

        # Show insights
        print("\nKey Insights:")
        stats = result.agg(
            F.min("average_amount").alias("min_avg"),
            F.max("average_amount").alias("max_avg"),
            F.avg("average_amount").alias("overall_avg"),
        ).collect()[0]

        print(f"- Lowest average transaction: ${stats['min_avg']:.2f}")
        print(f"- Highest average transaction: ${stats['max_avg']:.2f}")
        print(f"- Overall average across all merchants: ${stats['overall_avg']:.2f}")

        # Show top merchants by average transaction
        print("\nTop 10 Merchants by Average Transaction Value:")
        result.limit(10).show(truncate=False)

        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            save_results(result, output_path, format="csv")
            print(f"\n✓ Results saved to {output_path}")

        print("\nFull Results Sample (rows 11-30):")
        result.collect()[10:30]  # Show next 20 rows
        for row in result.collect()[10:30]:
            merchant = row["merchant_name"][:40]
            state = row["state_id"]
            avg = row["average_amount"]
            print(f"{merchant:<40} | {state:<10} | ${avg:>10.2f}")

        return result

    def run_task3(self, output_path: str | None = None) -> DataFrame:
        """
        Execute Task 3: Identify top 3 hours for largest sales per category.

        This task analyzes hourly sales patterns to determine the peak business
        hours for each product category, helping optimize operational hours
        and staffing decisions.

        Args:
            output_path: Optional path to save results as CSV. If not provided,
                results are only displayed and/or saved to Hive.

        Returns:
            DataFrame: Results containing columns:
                - category: Product category name
                - hour: Hour of day (0-23) when sales peak
        """
        print("\n" + "=" * 60)
        print("TASK 3: TOP 3 HOURS BY CATEGORY")
        print("=" * 60)

        df = self.get_cleaned_data()

        print("\nAnalyzing peak shopping hours by product category...")
        result = self.analysis.task3_top_hours_by_category(df)

        print("\nResult Schema:")
        result.printSchema()

        total_categories = result.select("category").distinct().count()
        print(f"\nTotal categories analyzed: {total_categories}")
        print(f"Total result records: {result.count()} (3 hours per category)")

        # Show insights by grouping hours
        print("\nPeak Shopping Hours Distribution:")
        hour_counts = result.groupBy("hour").count().orderBy("hour")
        hour_distribution = hour_counts.collect()

        print("\nHour | Categories with peak sales")
        print("-" * 35)
        for row in hour_distribution:
            hour = int(row["hour"])
            count = row["count"]
            time_label = f"{hour:02d}:00-{hour:02d}:59"
            bar = "█" * count
            print(f"{time_label} | {bar} ({count})")

        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            save_results(result, output_path, format="csv")
            print(f"\n✓ Results saved to {output_path}")

        print("\nPeak Hours by Category:")
        result.show(truncate=False)

        return result

    def run_task4(
        self, output_merchants: str | None = None, output_categories: str | None = None
    ) -> tuple[DataFrame, DataFrame]:
        """
        Execute Task 4: Analyze popular merchants and location-category correlation.

        This task performs two analyses:
        1. Identifies the top 10 most popular merchants in each city by transaction count
        2. Determines the dominant product category for each city

        Args:
            output_merchants: Optional path to save merchant popularity results as CSV
            output_categories: Optional path to save city category analysis as CSV

        Returns:
            tuple: Contains two DataFrames:
                - merchants_result: Top merchants by city with columns:
                    - merchant_name: Name of the merchant
                    - city_id: City identifier
                    - transaction_count: Number of transactions
                    - rank: Ranking within the city (1-10)
                - categories_result: Dominant category per city with columns:
                    - city_id: City identifier
                    - dominant_category: Most popular category in the city
                    - category_transactions: Transaction count for that category
        """
        print("\n" + "=" * 60)
        print("TASK 4: POPULAR MERCHANTS AND LOCATION ANALYSIS")
        print("=" * 60)

        df = self.get_cleaned_data()

        print("\nAnalyzing merchant popularity and city-category correlations...")
        analysis_results = self.analysis.task4_popular_merchants_location_analysis(df)
        merchants_result, categories_result = analysis_results

        print("\n--- Part 1: Popular Merchants by City ---")
        print("\nMerchants Result Schema:")
        merchants_result.printSchema()

        total_cities = merchants_result.select("city_id").distinct().count()
        print(f"\nAnalysis covers {total_cities} cities")
        merchant_count = merchants_result.count()
        print(f"Total result records: {merchant_count} (top 10 merchants per city)")

        # Show most popular merchants overall
        print("\nMost Popular Merchants Across All Cities:")
        overall_popular = (
            merchants_result.filter(F.col("rank") == 1)
            .orderBy(F.desc("transaction_count"))
            .limit(5)
        )
        overall_popular.show(truncate=False)

        print("\n--- Part 2: Dominant Categories by City ---")
        print("\nCategories Result Schema:")
        categories_result.printSchema()

        print(f"\nTotal cities analyzed: {categories_result.count()}")

        # Show category distribution
        print("\nCategory Dominance Distribution:")
        category_dist = (
            categories_result.groupBy("dominant_category").count().orderBy(F.desc("count"))
        )
        print("\nCategory | Number of Cities Where Dominant")
        print("-" * 50)
        for row in category_dist.collect():
            print(f"{row['dominant_category']:<30} | {row['count']:>5} cities")

        if output_merchants:
            os.makedirs(os.path.dirname(output_merchants), exist_ok=True)
            save_results(merchants_result, output_merchants, format="csv")
            print(f"\n✓ Merchant results saved to {output_merchants}")

        if output_categories:
            os.makedirs(os.path.dirname(output_categories), exist_ok=True)
            save_results(categories_result, output_categories, format="csv")
            print(f"✓ Category results saved to {output_categories}")

        print("\nTop Merchants by City (first 20):")
        merchants_result.filter(F.col("rank") <= 3).show(20, truncate=False)

        print("\nDominant Categories by City (sample):")
        categories_result.show(20, truncate=False)

        return merchants_result, categories_result

    def run_task5(self, output_dir: str | None = None) -> dict[str, DataFrame]:
        """
        Execute Task 5: Generate comprehensive business recommendations for new merchants.

        This task provides data-driven insights for new merchants entering the market,
        including optimal locations, product categories, operating hours, and
        installment payment strategies based on historical transaction analysis.

        Args:
            output_dir: Optional directory to save all recommendation files.
                Creates subdirectories for different recommendation aspects.

        Returns:
            dict: Dictionary containing recommendation DataFrames:
                - top_cities: Top 5 cities by total sales and transaction metrics
                - top_categories: Top 5 product categories by performance
                - monthly_trends: Sales trends by month showing seasonality
                - hourly_patterns: Sales distribution by hour of day
                - installment_recommendation: Profitability analysis of installment options
        """
        print("\n" + "=" * 60)
        print("TASK 5: BUSINESS RECOMMENDATIONS FOR NEW MERCHANTS")
        print("=" * 60)

        df = self.get_cleaned_data()

        print("\nGenerating comprehensive business insights...")
        recommendations = self.analysis.task5_business_recommendations(df)

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

            top_cities = recommendations["top_cities"]
            save_results(top_cities, f"{output_dir}/top_cities.csv", format="csv")

            top_categories = recommendations["top_categories"]
            save_results(top_categories, f"{output_dir}/top_categories.csv", format="csv")

            monthly = recommendations["monthly_trends"]
            save_results(monthly, f"{output_dir}/monthly_trends.csv", format="csv")

            hourly = recommendations["hourly_patterns"]
            save_results(hourly, f"{output_dir}/hourly_patterns.csv", format="csv")

            installments = recommendations["installment_recommendation"]
            save_results(installments, f"{output_dir}/installment_analysis.csv", format="csv")

            print(f"All recommendations saved to {output_dir}")

        print("\n" + "=" * 50)
        print("📊 BUSINESS RECOMMENDATIONS SUMMARY")
        print("=" * 50)

        print("\n1️⃣  TOP CITIES TO FOCUS ON:")
        print("\nSchema:")
        recommendations["top_cities"].printSchema()
        print("\nRecommended Cities:")
        recommendations["top_cities"].show(5, truncate=False)

        # Calculate market share
        total_sales = df.agg(F.sum("purchase_amount")).collect()[0][0]
        top_cities_sales = recommendations["top_cities"].agg(F.sum("total_sales")).collect()[0][0]
        market_share = (top_cities_sales / total_sales) * 100
        print(f"\n💡 Insight: Top 5 cities represent {market_share:.1f}% of total market")

        print("\n2️⃣  RECOMMENDED PRODUCT CATEGORIES:")
        print("\nSchema:")
        recommendations["top_categories"].printSchema()
        print("\nTop Categories:")
        recommendations["top_categories"].show(5, truncate=False)

        print("\n3️⃣  MONTHLY SALES TRENDS:")
        print("\nSchema:")
        recommendations["monthly_trends"].printSchema()

        # Show trend analysis
        monthly = recommendations["monthly_trends"].orderBy("year", "month").collect()
        print("\nMonthly Sales Trend:")
        print("Year-Month | Total Sales    | Transactions | Trend")
        print("-" * 60)

        prev_sales = 0
        for i, row in enumerate(monthly[-12:]):  # Show last 12 months
            trend = "" if i == 0 else ("↑" if row["total_sales"] > prev_sales else "↓")
            year_month = f"{row['year']}-{row['month']:02d}"
            sales = row["total_sales"]
            trans = row["transaction_count"]
            print(f"{year_month}     | ${sales:>12,.0f} | {trans:>11,} | {trend}")
            prev_sales = row["total_sales"]

        print("\n4️⃣  RECOMMENDED OPERATING HOURS:")
        print("\nSchema:")
        recommendations["hourly_patterns"].printSchema()

        print("\nPeak Business Hours:")
        hourly = recommendations["hourly_patterns"].orderBy(F.desc("total_sales")).limit(10)
        print("\nHour  | Sales Volume   | Transactions | Visual")
        print("-" * 60)

        max_sales = hourly.first()["total_sales"]
        for row in hourly.collect():
            hour = int(row["hour"])
            bar_length = int((row["total_sales"] / max_sales) * 30)
            bar = "█" * bar_length
            sales = row["total_sales"]
            trans = row["transaction_count"]
            print(f"{hour:02d}:00 | ${sales:>13,.0f} | {trans:>11,} | {bar}")

        print("\n5️⃣  INSTALLMENT PAYMENT ANALYSIS:")
        print("\nSchema:")
        recommendations["installment_recommendation"].printSchema()
        print("\nProfitability by Payment Terms:")
        recommendations["installment_recommendation"].show(truncate=False)

        # Key recommendation
        best_installment = (
            recommendations["installment_recommendation"].orderBy(F.desc("net_profit")).first()
        )
        installments = best_installment["installments"]
        profit_margin = best_installment["profit_margin_pct"]
        print(f"\n💡 Key Recommendation: Optimal installment plan is {installments} payment(s)")
        print(f"   Expected profit margin: {profit_margin:.1f}%")

        return recommendations

    def run_all_tasks(self, output_base_dir: str | None = None) -> None:
        """
        Execute all analysis tasks in sequence.

        This method runs all five analysis tasks sequentially, saving results
        to organized output directories. It's useful for batch processing
        and generating complete analysis reports.

        Args:
            output_base_dir: Base directory for all output files.
                Defaults to 'reports/'. Each task will create its own
                subdirectory or file within this base directory.
        """
        if output_base_dir:
            base = Path(output_base_dir)
        else:
            base = Path("reports")

        print("\n" + "=" * 60)
        print("Running all Billups Merchant Data Analysis tasks...")
        print("=" * 60)

        self.run_task1(str(base / "task1_top_merchants.csv"))
        print("\n" + "-" * 60)

        self.run_task2(str(base / "task2_avg_sales_by_state.csv"))
        print("\n" + "-" * 60)

        self.run_task3(str(base / "task3_top_hours_by_category.csv"))
        print("\n" + "-" * 60)

        self.run_task4(
            str(base / "task4_popular_merchants.csv"), str(base / "task4_city_categories.csv")
        )
        print("\n" + "-" * 60)

        self.run_task5(str(base / "task5_recommendations"))

        print("\n" + "=" * 60)
        print("All tasks completed successfully!")
        print("=" * 60)

    def stop(self) -> None:
        """
        Stop the Spark session and release resources.

        This method should be called when all processing is complete to ensure
        proper cleanup of Spark resources. It's automatically called in the
        main() function but should be explicitly called if using the class directly.
        """
        if self.spark:
            self.spark.stop()


def main() -> None:
    """
    Main entry point for the Spark job command-line interface.

    Parses command-line arguments, validates input files, configures Spark settings,
    and executes the requested analysis tasks. Handles both local and distributed
    execution modes with proper error handling and resource cleanup.
    """
    parser = argparse.ArgumentParser(
        description="Billups Merchant Data Analysis Spark Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tasks
  python -m src.spark_job -t data/transactions.parquet -m data/merchants.csv --task all

  # Run specific task
  python -m src.spark_job -t data/transactions.parquet -m data/merchants.csv --task 1

  # Run without Hive
  python -m src.spark_job -t data/transactions.parquet \
      -m data/merchants.csv --task all --no-hive

  # Specify output directory
  python -m src.spark_job -t data/transactions.parquet \
      -m data/merchants.csv --task all -o custom_reports/
        """,
    )

    parser.add_argument(
        "-t", "--transactions", required=True, help="Path to historical_transactions.parquet file"
    )
    parser.add_argument("-m", "--merchants", required=True, help="Path to merchants.csv file")
    parser.add_argument(
        "--task",
        required=True,
        choices=["1", "2", "3", "4", "5", "all"],
        help="Which task to run (1-5 or all)",
    )
    parser.add_argument("-o", "--output", help="Output directory for results (default: reports/)")
    parser.add_argument(
        "--no-hive",
        action="store_true",
        default=False,
        help="Disable Hive support (Hive is enabled by default)",
    )
    parser.add_argument(
        "--spark-master", help="Spark master URL (e.g., local[4], spark://host:port)"
    )
    parser.add_argument(
        "--spark-executor-memory", default="2g", help="Executor memory (default: 2g)"
    )
    parser.add_argument("--spark-executor-cores", default="2", help="Executor cores (default: 2)")

    args = parser.parse_args()

    if not os.path.exists(args.transactions):
        print(f"Error: Transactions file not found: {args.transactions}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.merchants):
        print(f"Error: Merchants file not found: {args.merchants}", file=sys.stderr)
        sys.exit(1)

    spark_config = {}
    if args.spark_master:
        spark_config["spark.master"] = args.spark_master
    spark_config["spark.executor.memory"] = args.spark_executor_memory
    spark_config["spark.executor.cores"] = args.spark_executor_cores

    job = SparkJob(
        args.transactions,
        args.merchants,
        # Invert the flag since --no-hive means use_hive=False
        use_hive=not args.no_hive,
        spark_config=spark_config if spark_config else None,
    )

    try:
        if args.task == "all":
            job.run_all_tasks(args.output)
        elif args.task == "1":
            output = f"{args.output or 'reports'}/task1_top_merchants.csv"
            job.run_task1(output)
        elif args.task == "2":
            output = f"{args.output or 'reports'}/task2_avg_sales_by_state.csv"
            job.run_task2(output)
        elif args.task == "3":
            output = f"{args.output or 'reports'}/task3_top_hours_by_category.csv"
            job.run_task3(output)
        elif args.task == "4":
            base = args.output or "reports"
            job.run_task4(
                f"{base}/task4_popular_merchants.csv", f"{base}/task4_city_categories.csv"
            )
        elif args.task == "5":
            output = f"{args.output or 'reports'}/task5_recommendations"
            job.run_task5(output)
    finally:
        job.stop()


if __name__ == "__main__":
    main()
