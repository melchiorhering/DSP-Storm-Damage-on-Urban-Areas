import polars as pl
from dagster import AssetExecutionContext, AssetIn, asset, get_dagster_logger


@asset(
    name="xgboost",
    key_prefix="model",
    ins={"categorical_data": AssetIn(key="categorical_data")},
    io_manager_key="database_io_manager",
    description="model run with XGBoost",
)
def xgb_boost(
    context: AssetExecutionContext, categorical_data: pl.DataFrame
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # # SELECT ALL ROWS WITH INCIDENTS == 0
    # selection_eq_0 = wijken_weer_bomen_incidenten.filter(
    #     pl.col("Total_Incidents") == 0
    # ).drop("wijkcode", "Total_Incidents")
    # logger.info(selection_eq_0.shape)
    # logger.info(selection_eq_0.head(5))

    # # SELECT ONLY ROWS WITH INCIDENTS> 0
    # selection_gt_0 = wijken_weer_bomen_incidenten.filter(
    #     pl.col("Total_Incidents") > 0
    # ).drop("wijkcode")
    # logger.info(selection_gt_0.shape)
    # logger.info(selection_gt_0.head(5))
    # # 'EXPLODE' THOSE ROWS SO EACH ROW REPRESENTS AN Type of Inicdents
    # selection_gt_0_explode = selection_gt_0.select(
    #     pl.exclude("Total_Incidents").repeat_by("Total_Incidents").explode()
    # )
    # logger.info(selection_gt_0_explode.shape)
    # logger.info(selection_gt_0_explode.head(5))

    # # CONCAT TO COMPLETE DF
    # final_df = pl.concat([selection_gt_0_explode, selection_eq_0])
    # logger.info(final_df.shape)
    # logger.info(final_df.head(5))

    # context.add_output_metadata(
    #     metadata={
    #         "number_of_columns": MetadataValue.int(len(final_df.columns)),
    #         "preview": MetadataValue.md(final_df.head().to_pandas().to_markdown()),
    #     }
    # )
    return pl.DataFrame()
