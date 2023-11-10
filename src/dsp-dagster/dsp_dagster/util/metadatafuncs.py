# from typing import OrderedDict
# from dagster import TableColumn, TableSchema
# from polars.datatypes import dtype_to_py_type


# def table_schema_polars(df_schema: OrderedDict):
#     """Generate a TableSchema based on the schema of a Polars DataFrame.

#     :param df_schema OrderedDict: The Polars dataframe schema
#     :return TableSchema: A TableSchema representing the columns and their types in the DataFrame.
#     """
#     schema = TableSchema(
#         columns=[
#             TableColumn(name=col, type=dtype_to_py_type(df_schema[col]))
#             for col in df_schema
#         ]
#     )

#     return schema
