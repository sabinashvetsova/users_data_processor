import csv
import json
import os
from datetime import datetime
import pandas as pd

from minio import Minio
from minio.select import (
    SelectRequest,
    CSVInputSerialization,
    JSONOutputSerialization,
    CSVOutputSerialization,
)

from app.utils import get_where_clause

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")


class DataProcessor:
    def __init__(self):
        self.client = Minio(
            "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
        )

    def update_data(self):
        rows = []
        csv_files = []
        png_files = []
        files = self.client.list_objects("datalake", prefix="02-src-data/")

        for file in files:
            if file.object_name.endswith(".csv"):
                csv_files.append(file.object_name)
            elif file.object_name.endswith(".png"):
                png_files.append(file.object_name)

        for file in csv_files:
            user_id = file.split("/")[-1].split(".")[0]
            obj = self.client.get_object("datalake", file)
            df = pd.read_csv(obj, skipinitialspace=True)
            row = df.iloc[0]
            img_path = (
                f"{user_id}.png" if f"02-src-data/{user_id}.png" in png_files else ""
            )
            rows.append(
                [user_id, row["first_name"], row["last_name"], row["birthts"], img_path]
            )

        with open("output.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                ["user_id", "first_name", "last_name", "birthts", "img_path"]
            )
            writer.writerows(rows)

        self.client.fput_object("datalake", "output.csv", "output.csv")

    def get_users(self, is_image_exists, min_age, max_age):
        where = get_where_clause(is_image_exists, min_age, max_age)

        with self.client.select_object_content(
            "datalake",
            "output.csv",
            SelectRequest(
                f"select * from S3Object {where}",
                CSVInputSerialization(file_header_info="USE"),
                JSONOutputSerialization(record_delimiter=","),
                request_progress=True,
            ),
        ) as result:
            for data in result.stream():
                users = json.loads(f"[{data.decode('utf8').strip(',')}]")

        return users

    def get_average_age(self, is_image_exists, min_age, max_age):
        where = get_where_clause(is_image_exists, min_age, max_age)

        with self.client.select_object_content(
            "datalake",
            "output.csv",
            SelectRequest(
                f"select AVG(birthts) from S3Object {where}",
                CSVInputSerialization(file_header_info="USE"),
                CSVOutputSerialization(),
                request_progress=True,
            ),
        ) as result:
            for data in result.stream():
                result = data.decode()

        if result.strip():
            average_birth_ts = float(result)
            current_year = datetime.now().year
            average_birth_year = datetime.fromtimestamp(average_birth_ts / 1000.0).year
            average_age = current_year - average_birth_year
        else:
            average_age = None

        return {"average_age": average_age}
