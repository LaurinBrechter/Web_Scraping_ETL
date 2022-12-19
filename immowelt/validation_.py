import pandas as pd
import tensorflow_data_validation as tfdv


df = pd.read_csv("data/1_scraping_results.csv")
# tfdv.generate_statistics_from_csv("data/1_scraping_results.csv", delimiter=",")
stats = tfdv.generate_statistics_from_dataframe(df)
# tfdv.visualize_statistics(stats)
schema = tfdv.infer_schema(statistics=stats)
tfdv.display_schema(schema)