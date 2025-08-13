import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    from pathlib import Path
    import marimo as mo
    import polars as pl
    from utils.load_google_sheet import load_google_sheet
    from utils.save.to_parquet import write_parquet_if_changed


@app.cell
def _():
    price_df = load_google_sheet(config_name="price").data.select(
        pl.col("Service").cast(dtype=pl.Utf8).alias("operation_type"),
        pl.col("Price").cast(dtype=pl.Float64).alias("price"),
        pl.col("StartingDate").alias("effective_date")
    ).collect()

    price_df.write_parquet("./dataset/price_data.parquet")
    return (price_df,)


@app.cell
def _(price_df):
    write_parquet_if_changed(df=price_df,verbose=False)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
