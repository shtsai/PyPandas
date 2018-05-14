import re
import pyspark.sql
from pyspark.sql.functions import udf
from pypandas.core import apply_udf
import pypandas.common_patterns as patterns

class TextCleaner:
    def __init__(self):
        self.clean_functions = []

    def register(self, clean_function):
        self.clean_functions.append(clean_function)

    def register_re_sub(self, to_replace, value):
        def clean_function(dataframe, columns):
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
        self.register(clean_function)

    def clean(self, dataframe, columns):
        cleaned_dataframe = dataframe
        for clean_function in self.clean_functions:
            cleaned_dataframe = clean_function(cleaned_dataframe, columns)
        return cleaned_dataframe

def clean_text(dataframe, columns):
    cleaner = TextCleaner()
    cleaner.register_re_sub(patterns.LEADING_SPACE, '')
    cleaner.register_re_sub(patterns.TRAILING_SPACE, '')
    cleaner.register_re_sub(patterns.URL, '_url_')
    cleaner.register_re_sub(patterns.BLANKS, '_')
    cleaner.register_re_sub(patterns.NOT_A_WORD, '')
    cleaner.register_re_sub(patterns.NUMBER, '_number_')
    cleaner.register_re_sub(patterns.CONSECUTIVE_SPACE, ' ')
    return cleaner.clean(dataframe, columns)

def clean_url(dataframe, columns, value=''):
    return sub_with_pattern(dataframe, columns, patterns.URL, value)

def clean_leading_space(dataframe, columns):
    return sub_with_pattern(dataframe, columns, patterns.LEADING_SPACE, '')

def clean_trailing_space(dataframe, columns):
    return sub_with_pattern(dataframe, columns, patterns.TRAILING_SPACE, '')

def clean_consecutive_space(dataframe, columns):
    return sub_with_pattern(dataframe, columns, patterns.CONSECUTIVE_SPACE, ' ')

def replace_number(dataframe, columns, value='_number_'):
    return sub_with_pattern(dataframe, columns, patterns.NUMBER, value)

def clean_not_a_word(dataframe, columns):
    return sub_with_pattern(dataframe, columns, patterns.NOT_A_WORD, '')

def clean_blank(dataframe, columns):
    return sub_with_pattern(dataframe, columns, patterns.BLANKS, '_')

def sub_with_pattern(dataframe, columns, to_replace, value):
    cleaner = TextCleaner()
    cleaner.register_re_sub(to_replace, value)
    return cleaner.clean(dataframe, columns)
