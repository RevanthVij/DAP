from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd

postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/postgres"

@op(ins={'start': In(None)},out=Out(bool))
def load_IRL_SH_TBL(start):
    logger = get_dagster_logger()
    IRL_S = pd.read_csv("D:/DAP/ca samples/AUTO/transformed_IRL_SH.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE IRL_SHC;")
        rowcount = IRL_S.to_sql(
            name="IRL_SH",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False