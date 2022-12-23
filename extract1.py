from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd

mongo_connection_string = "mongodb://dap:dap@127.0.0.1"

### IRELAND SOCIAL_HOUSING###

SHPP_columns = {"Approved Housing Body": "Approved_Housing_Body",
                "Completed": "Completed",
                "Funding Programme": "Funding_Programme",
                "LA": "LA",
                "No_ of Units":"No_of_Units",
                "On Site": "On_Site",
                "Scheme/Project Name": "Scheme_Project_Name",
                "Stage 1 Capital Appraisal": "Stage_1_Capital_Appraisal",
                "Stage 2 Pre Planning": "Stage_2_Pre_Planning",
                "Stage 3 Pre Tender design": "Stage_3_Pre_Tender_design",
                "Stage 4 Tender Report or Final Turnkey/CALF approval": "Stage_4_Tender_Report_or_Final_Turnkey_CALF_approval",
                }

IRL_SHDataFrame = create_dagster_pandas_dataframe_type(
    name="IRL_SHDataFrame",
    columns=[
        PandasColumn.string_column("Approved_Housing_Body", non_nullable=False, unique=False),
        PandasColumn.string_column("Completed", non_nullable=False),
        PandasColumn.string_column("Funding_Programme", non_nullable=False),
        PandasColumn.string_column("LA", non_nullable=False),
        PandasColumn.integer_column("No_of_Units", non_nullable=False),
        PandasColumn.string_column("On_Site", non_nullable=False),
        PandasColumn.string_column("Scheme_Project_Name", non_nullable=False),
        PandasColumn.string_column("Stage_1_Capital_Appraisal", non_nullable=False),
        PandasColumn.string_column("Stage_2_Pre_Planning", non_nullable=False),
        PandasColumn.string_column("Stage_3_Pre_Tender_design", non_nullable=False),
        PandasColumn.string_column("Stage_4_Tender_Report_or_Final_Turnkey_CALF_approval", non_nullable=False),
    ],
)


@op(ins={'start': In(bool)}, out=Out(IRL_SHDataFrame))
def extract_IRL_SH(start) -> IRL_SHDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["DAPGRPM_database"]
    IRL_SH = pd.DataFrame(db.social_housing_construction.find({}))
    IRL_SH.drop(
        columns=["_id", "No_"],
        axis=1,
        inplace=True
    )
    IRL_SH.rename(
        columns=SHPP_columns,
        inplace=True
    )
    conn.close()
    return IRL_SH


@op(ins={'IRL_SH': In(IRL_SHDataFrame)}, out=Out(None))
def stage_extracted_IRL_SH(IRL_SH):
    IRL_SH.to_csv("D:/DAP/ca samples/AUTO/IRL_SH.csv", index=False, sep="\t")
