
def apply_udf_to_columns(dataframe, columns, udf):
    expression_cols = []
    for col in dataframe.columns:
        if col in columns:
            expression_cols.append(udf(col).alias(col))
        else:
            expression_cols.append(col)

    return dataframe.select(expression_cols)

def apply_udf_to_column(dataframe, column, udf):
    return dataframe.withColumn(column, udf(column))
