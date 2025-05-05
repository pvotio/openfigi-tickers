from config import logger, settings
from database.helper import init_db_instance
from engine.core import Core
from transformer import Transformer


def main():
    logger.info("Initializing Scraper Engine")
    core = Core()
    dataframe = core.run()
    logger.info("Transforming Data")
    agent = Transformer(dataframe)
    transformed_dataframe = agent.transform()
    transformed_dataframe.to_csv("openfigi_transformed.csv")
    logger.info(f"\n\n{transformed_dataframe}")
    logger.info("Preparing Database Inserter")
    conn = init_db_instance()
    logger.info(f"Inserting Data into {settings.OUTPUT_TABLE}")
    conn.insert_table(transformed_dataframe, settings.OUTPUT_TABLE)
    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
