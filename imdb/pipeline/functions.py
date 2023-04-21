from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import DataType

from imdb.ioutil import eprint
from imdb.pipeline import columns as c


def apply_with_columns_func(df: DataFrame, columns, func):
    action_map = {column: func(column) for column in columns}
    return df.withColumns(action_map)


def test_data_complaince(df: DataFrame, col_name: str, column_type: DataType, null_value, show_details=False):
    # print("Check data complaince")
    test_df = (df.filter((f.col(col_name) != null_value) &
                         f.col(col_name).cast(column_type).isNull()))
    # print("filter is applied")
    if not test_df.isEmpty():
        eprint("Found wrong rows")
        if show_details:
            eprint(f"Wrong rows count: {test_df.count()}")
            eprint("Wrong rows: ")
            test_df.orderBy(f.col(col_name)).show()
        return False
    return True


def count_nulls(df, *col_names):
    nulls_df = df.select(f.count(f.when(f.col(c.ta_title).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_region).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_language).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_types).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_attributes).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_isOriginalTitle).isNull(), 1))
                         )
