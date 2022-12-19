import os
from airflow.decorators import dag, task
from main import main
import pandas as pd
import numpy as np
import pendulum
import cloudscraper
import datetime

@dag(
    tags=["example"],
    start_date=pendulum.datetime(2021, 12, 1, tz="UTC"),
)
def immowelt_dag():
    
    @task()
    def scrape():

        current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H")
        scraper = cloudscraper.create_scraper()
        data = main(
            "Documents/code/Web_Scraping_ETL/immowelt/data",
            "https://www.immowelt.de/liste/dresden/wohnungen/kaufen?d=true&sd=DESC&sf=RELEVANCE&sp=",
            scraper,
            True,
            current_time
            
        )
        return {"data": data}

    @task()
    def postprocess(data:pd.DataFrame):
        data.area = data.area.str.replace(",", ".")
        data.loc[data.area == "k.A.", "area"] = np.nan
        data.area = data.area.astype(float)
        data.loc[data.price == "auf", "price"] = np.nan
        data.price = data.price.astype(float)

        return {"data": data}

    @task()
    def load(data:pd.DataFrame):
        data.to_csv("Documents/code/Web_Scraping_ETL/immowelt/processed_data/test.csv")

    data = scrape()
    data = postprocess(data["data"])
    load(data["data"])


immowelt_dag()