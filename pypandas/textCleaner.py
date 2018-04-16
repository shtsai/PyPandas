import re
import pyspark.sql
from pyspark.sql.functions import udf
from pypandas.core import apply_udf

def clean_text(dataframe, columns):
    df = clean_leading_space(dataframe, columns)
    df = clean_trailing_space(df, columns)
    df = clean_url(df, columns, '_url_')
    df = clean_blank(df, columns)
    df = clean_not_a_word(df, columns)
    df = replace_number(df, columns)
    df = clean_consecutive_space(df, columns)
    return df

def clean_url(dataframe, columns, value=''):
    url = "(http:\/\/www\.|https:\/\/www\.|http:\/\/|https" \
          + ":\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{" \
          + "2,5}(:[0-9]{1,5})?(\/.*)?"
    return sub_with_pattern(dataframe, columns, url, value)

def clean_leading_space(dataframe, columns):
    leading_space = "^ +"
    return sub_with_pattern(dataframe, columns, leading_space, '')

def clean_trailing_space(dataframe, columns):
    trailing_space = " +$"
    return sub_with_pattern(dataframe, columns, trailing_space, '')

def clean_consecutive_space(dataframe, columns):
    consecutive_space = " +"
    return sub_with_pattern(dataframe, columns, consecutive_space, ' ')

def replace_number(dataframe, columns, value='_number_'):
    number = "\d+"
    return sub_with_pattern(dataframe, columns, number, value)

def clean_not_a_word(dataframe, columns):
    not_a_word = "[^\w\d\s]+"
    return sub_with_pattern(dataframe, columns, not_a_word, '')

def clean_blank(dataframe, columns):
    blanks = "_+"
    return sub_with_pattern(dataframe, columns, blanks, '_')

def sub_with_pattern(dataframe, columns, to_replace, value):
    def re_sub_function(datum):
        if datum is not None:
            if type(datum) is str:
                return re.sub(to_replace, value, datum)
            else:
                return datum
        else:
            return None

    re_udf = udf(re_sub_function)
    return apply_udf(dataframe, columns, re_udf)
