import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class SparkApp:
    def __init__(self, config_path):
        self.config = self.read_config(config_path)
        self.spark = self.create_spark_session()

    def read_config(self, file_path):
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config

    def create_spark_session(self):
        spark_config = self.config["spark"]
        spark = SparkSession.builder \
            .appName(spark_config["app_name"]) \
            .master(spark_config["master"]) \
            .config("spark.executor.memory", spark_config["executor_memory"]) \
            .config("spark.executor.cores", spark_config["executor_cores"]) \
            .getOrCreate()
        return spark

    def read_data(self):
        input_tables = self.config["data"]["input_tables"]
        dataframes = {}
        for table in input_tables:
            df = self.spark.read.format(table["format"]).load(table["path"])
            dataframes[table["name"]] = df
        return dataframes

    def transform_data(self, dataframes):
        transformations = self.config["transformations"]
        for table_name, df in dataframes.items():
            for rename_col in transformations["rename_columns"]:
                df = df.withColumnRenamed(rename_col["old_name"], rename_col["new_name"])
            df = df.filter(transformations["filter_condition"])
            dataframes[table_name] = df
        return dataframes

    def write_data(self, dataframes):
        output_path = self.config["data"]["output_path"]
        output_format = self.config["data"]["output_format"]
        for table_name, df in dataframes.items():
            df.write.format(output_format).save(f"{output_path}/{table_name}")

    def run(self):
        dataframes = self.read_data()
        transformed_dataframes = self.transform_data(dataframes)
        self.write_data(transformed_dataframes)


if __name__ == "__main__":
    app = SparkApp("conf.json")
    app.run()
