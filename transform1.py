from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

#### TRANSFORM IRL_SH ####

Transform_IRL_SHDataFrame = create_dagster_pandas_dataframe_type(
    name="Transform_IRL_SHDataFrame",
    columns=[
        PandasColumn.string_column("Approved_Housing_Body", non_nullable=False, unique=False),
        PandasColumn.string_column("Funding_Programme", non_nullable=False),
        PandasColumn.string_column("LA", non_nullable=False),
        PandasColumn.integer_column("No_of_Units", non_nullable=False),
        PandasColumn.string_column("Scheme_Project_Name", non_nullable=False),
       
    ],
)


@op(ins={'start': In(None)}, out=Out(Transform_IRL_SHDataFrame))
def transform_extracted_IRL_SH(start) -> Transform_IRL_SHDataFrame:
    T_IRL_SH = pd.read_csv("D:/DAP/ca samples/AUTO/IRL_SH.csv", sep="\t")
    T_IRL_SH = pd.melt(T_IRL_SH, id_vars=['Funding_Programme', 'LA', 'Scheme_Project_Name', 'No_of_Units',
       'Approved_Housing_Body'],var_name='Stage', value_name='Date')
    T_IRL_SH['Date'] = T_IRL_SH['Date'].str.replace('Q1-', '0331')
    T_IRL_SH['Date'] = T_IRL_SH['Date'].str.replace('Q2-', '0630')
    T_IRL_SH['Date'] = T_IRL_SH['Date'].str.replace('Q3-', '0930')
    T_IRL_SH['Date'] = T_IRL_SH['Date'].str.replace('Q4-', '1231')
    T_IRL_SH['Date'] = pd.to_datetime(T_IRL_SH['Date'], format='%m%d%Y', errors='coerce')
    T_IRL_SH['Approved_Housing_Body'] = T_IRL_SH['Approved_Housing_Body'].str.replace('ï¿½', '')

    return T_IRL_SH


@op(ins={'T_IRL_PP': In(Transform_IRL_SHDataFrame)}, out=Out(None))
def stage_transformed_IRL_SH(T_IRL_PP):
    T_IRL_PP.to_csv("D:/DAP/ca samples/AUTO/transformed_IRL_SH.csv", sep="\t", index=False)
