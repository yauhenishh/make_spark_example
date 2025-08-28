import os
import sys

import click

from src.analysis.tasks import MerchantAnalysis
from src.data.loader import DataLoader
from src.utils.hive_utils import get_spark_with_hive, write_to_hive
from src.utils.spark_utils import save_results


@click.group()
@click.option(
    "--transactions", "-t", required=True, help="Path to historical_transactions.parquet file"
)
@click.option("--merchants", "-m", required=True, help="Path to merchants.csv file")
@click.option("--use-hive/--no-hive", default=True, help="Enable Hive support for data storage")
@click.pass_context
def cli(ctx, transactions, merchants, use_hive):
    """Billups Data Analysis CLI - Analyze merchant transaction data"""
    ctx.ensure_object(dict)

    if not os.path.exists(transactions):
        click.echo(f"Error: Transactions file not found: {transactions}", err=True)
        sys.exit(1)

    if not os.path.exists(merchants):
        click.echo(f"Error: Merchants file not found: {merchants}", err=True)
        sys.exit(1)

    ctx.obj["transactions_path"] = transactions
    ctx.obj["merchants_path"] = merchants
    ctx.obj["use_hive"] = use_hive

    if use_hive:
        ctx.obj["spark"] = get_spark_with_hive()
    else:
        from src.utils.spark_utils import create_spark_session

        ctx.obj["spark"] = create_spark_session()

    ctx.obj["loader"] = DataLoader(ctx.obj["spark"], use_hive=use_hive)
    ctx.obj["analysis"] = MerchantAnalysis()


@cli.command()
@click.option("--output", "-o", default="reports/task1_top_merchants.csv", help="Output file path")
@click.pass_context
def task1(ctx, output):
    """Generate top 5 merchants by purchase amount for each month/city"""
    click.echo("Loading and cleaning data...")
    df = ctx.obj["loader"].get_cleaned_data(ctx.obj["transactions_path"], ctx.obj["merchants_path"])

    click.echo("Analyzing top merchants by city and month...")
    result = ctx.obj["analysis"].task1_top_merchants_by_city_month(df)

    click.echo(f"Saving results to {output}...")
    os.makedirs(os.path.dirname(output), exist_ok=True)
    save_results(result, output, format="csv")

    # Write to Hive if enabled
    if ctx.obj["use_hive"]:
        click.echo("Writing results to Hive table...")
        write_to_hive(result, "top_merchants_by_month_city")

    click.echo("Task 1 completed successfully!")
    result.show(50, truncate=False)


@cli.command()
@click.option(
    "--output", "-o", default="reports/task2_avg_sales_by_state.csv", help="Output file path"
)
@click.pass_context
def task2(ctx, output):
    """Calculate average sale amount per merchant per state"""
    click.echo("Loading and cleaning data...")
    df = ctx.obj["loader"].get_cleaned_data(ctx.obj["transactions_path"], ctx.obj["merchants_path"])

    click.echo("Calculating average sales by merchant and state...")
    result = ctx.obj["analysis"].task2_average_sale_by_merchant_state(df)

    click.echo(f"Saving results to {output}...")
    os.makedirs(os.path.dirname(output), exist_ok=True)
    save_results(result, output, format="csv")

    # Write to Hive if enabled
    if ctx.obj["use_hive"]:
        click.echo("Writing results to Hive table...")
        write_to_hive(result, "avg_sales_by_merchant_state")

    click.echo("Task 2 completed successfully!")
    result.show(50, truncate=False)


@cli.command()
@click.option(
    "--output", "-o", default="reports/task3_top_hours_by_category.csv", help="Output file path"
)
@click.pass_context
def task3(ctx, output):
    """Identify top 3 hours for largest sales per category"""
    click.echo("Loading and cleaning data...")
    df = ctx.obj["loader"].get_cleaned_data(ctx.obj["transactions_path"], ctx.obj["merchants_path"])

    click.echo("Finding top hours by category...")
    result = ctx.obj["analysis"].task3_top_hours_by_category(df)

    click.echo(f"Saving results to {output}...")
    os.makedirs(os.path.dirname(output), exist_ok=True)
    save_results(result, output, format="csv")

    # Write to Hive if enabled
    if ctx.obj["use_hive"]:
        click.echo("Writing results to Hive table...")
        write_to_hive(result, "top_hours_by_category")

    click.echo("Task 3 completed successfully!")
    result.show(50, truncate=False)


@cli.command()
@click.option(
    "--output-merchants",
    "-om",
    default="reports/task4_popular_merchants.csv",
    help="Output for popular merchants",
)
@click.option(
    "--output-categories",
    "-oc",
    default="reports/task4_city_categories.csv",
    help="Output for city categories",
)
@click.pass_context
def task4(ctx, output_merchants, output_categories):
    """Analyze popular merchants by location and category correlation"""
    click.echo("Loading and cleaning data...")
    df = ctx.obj["loader"].get_cleaned_data(ctx.obj["transactions_path"], ctx.obj["merchants_path"])

    click.echo("Analyzing merchant popularity and location-category correlation...")
    analysis = ctx.obj["analysis"]
    merchants_result, categories_result = analysis.task4_popular_merchants_location_analysis(df)

    click.echo(f"Saving merchant results to {output_merchants}...")
    os.makedirs(os.path.dirname(output_merchants), exist_ok=True)
    save_results(merchants_result, output_merchants, format="csv")

    click.echo(f"Saving category results to {output_categories}...")
    save_results(categories_result, output_categories, format="csv")

    # Write to Hive if enabled
    if ctx.obj["use_hive"]:
        click.echo("Writing results to Hive tables...")
        write_to_hive(merchants_result, "popular_merchants_by_city")
        write_to_hive(categories_result, "city_dominant_categories")

    click.echo("Task 4 completed successfully!")
    click.echo("\nTop merchants by city (sample):")
    merchants_result.show(20, truncate=False)
    click.echo("\nDominant categories by city:")
    categories_result.show(20, truncate=False)


@cli.command()
@click.option(
    "--output-dir", "-o", default="reports/task5_recommendations/", help="Output directory"
)
@click.pass_context
def task5(ctx, output_dir):
    """Generate business recommendations for new merchant"""
    click.echo("Loading and cleaning data...")
    df = ctx.obj["loader"].get_cleaned_data(ctx.obj["transactions_path"], ctx.obj["merchants_path"])

    click.echo("Generating business recommendations...")
    recommendations = ctx.obj["analysis"].task5_business_recommendations(df)

    os.makedirs(output_dir, exist_ok=True)

    click.echo("\n=== BUSINESS RECOMMENDATIONS ===\n")

    click.echo("a. TOP CITIES TO FOCUS ON:")
    top_cities = recommendations["top_cities"]
    save_results(top_cities, f"{output_dir}/top_cities.csv", format="csv")
    if ctx.obj["use_hive"]:
        write_to_hive(top_cities, "recommendation_top_cities")
    top_cities.show(5, truncate=False)

    click.echo("\nb. RECOMMENDED CATEGORIES TO SELL:")
    top_categories = recommendations["top_categories"]
    save_results(top_categories, f"{output_dir}/top_categories.csv", format="csv")
    if ctx.obj["use_hive"]:
        write_to_hive(top_categories, "recommendation_top_categories")
    top_categories.show(5, truncate=False)

    click.echo("\nc. MONTHLY SALES TRENDS:")
    monthly = recommendations["monthly_trends"]
    save_results(monthly, f"{output_dir}/monthly_trends.csv", format="csv")
    if ctx.obj["use_hive"]:
        write_to_hive(monthly, "monthly_sales_trends")
    monthly.show(20, truncate=False)

    click.echo("\nd. RECOMMENDED OPERATING HOURS:")
    hourly = recommendations["hourly_patterns"]
    save_results(hourly, f"{output_dir}/hourly_patterns.csv", format="csv")
    if ctx.obj["use_hive"]:
        write_to_hive(hourly, "hourly_sales_patterns")

    peak_hours = hourly.orderBy(hourly.total_sales.desc()).limit(10)
    click.echo("Peak business hours:")
    peak_hours.show(truncate=False)

    click.echo("\ne. INSTALLMENT PAYMENT ANALYSIS:")
    installments = recommendations["installment_recommendation"]
    save_results(installments, f"{output_dir}/installment_analysis.csv", format="csv")
    if ctx.obj["use_hive"]:
        write_to_hive(installments, "installment_profitability_analysis")
    installments.show(truncate=False)

    click.echo("\nTask 5 completed successfully!")


@cli.command()
@click.pass_context
def load_raw_data(ctx):
    """Load raw data into Hive tables"""
    if not ctx.obj["use_hive"]:
        click.echo("Error: Hive support must be enabled to load raw data", err=True)
        return

    click.echo("Loading raw data into Hive...")

    # Load raw transactions
    click.echo(f"Loading transactions from {ctx.obj['transactions_path']}...")
    transactions_df = ctx.obj["loader"].load_transactions(ctx.obj["transactions_path"])
    # Add year and month columns for partitioning
    from pyspark.sql import functions as F

    transactions_df = transactions_df.withColumn("year", F.year("purchase_date")).withColumn(
        "month", F.month("purchase_date")
    )
    write_to_hive(transactions_df, "raw_transactions", partition_by=["year", "month"])

    # Load raw merchants
    click.echo(f"Loading merchants from {ctx.obj['merchants_path']}...")
    merchants_df = ctx.obj["loader"].load_merchants(ctx.obj["merchants_path"])
    write_to_hive(merchants_df, "raw_merchants")

    # Load cleaned data
    click.echo("Creating cleaned data table...")
    cleaned_df = ctx.obj["loader"].get_cleaned_data(
        ctx.obj["transactions_path"], ctx.obj["merchants_path"]
    )
    cleaned_df = cleaned_df.withColumn("year", F.year("purchase_date")).withColumn(
        "month", F.month("purchase_date")
    )
    write_to_hive(cleaned_df, "cleaned_transactions", partition_by=["year", "month"])

    click.echo("Raw data loaded successfully!")


@cli.command()
@click.pass_context
def run_all(ctx):
    """Run all analysis tasks"""
    click.echo("Running all analysis tasks...")

    ctx.invoke(task1)
    click.echo("\n" + "=" * 50 + "\n")

    ctx.invoke(task2)
    click.echo("\n" + "=" * 50 + "\n")

    ctx.invoke(task3)
    click.echo("\n" + "=" * 50 + "\n")

    ctx.invoke(task4)
    click.echo("\n" + "=" * 50 + "\n")

    ctx.invoke(task5)

    click.echo("\nAll tasks completed successfully!")


def main():
    """Main entry point"""
    cli(obj={})


if __name__ == "__main__":
    main()
