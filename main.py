#%%
from helpers import *
import pandas as pd

clinical_tests = get_clinical_tests_available('clinical_tests_list.json')

files = get_files('./input/', 'pdf')

df_main = build_table_with_results(files, clinical_tests)

df_main = handle_dates(df_main, "dt_nascimento", "dt_coleta", "%d/%m/%Y")

df_main = combine_records(df_main)

df_main = clean_col_names(df_main)

df_main = rebuild_table_by_time(df_main, group_cols = ['nome', 'sexo', 'dt_nascimento']
                      ,time_col = "dt_coleta"
                      )

df_main = df_main.sort_values(["nome", "dt_nascimento"])

# %%
df_main.to_excel('results.xlsx', engine="openpyxl")

