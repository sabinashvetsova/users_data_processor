import csv
import glob
import json
import os
from datetime import datetime

from fastapi import FastAPI
from minio import Minio
from minio.select import (
    SelectRequest,
    CSVInputSerialization,
    CSVOutputSerialization,
    JSONOutputSerialization,
)

from app.utils import get_where_clause

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")

app = FastAPI()


@app.get("/data")
def get_data(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    client = Minio(
        "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    where = get_where_clause(is_image_exists, min_age, max_age)

    with client.select_object_content(
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

    return {"users": users}


@app.post("/data")
def post_data():
    rows = []
    csv_files = glob.glob("app/02-src-data/*.csv")
    for file in csv_files:
        user_id = file.split("/")[-1].split(".")[0]
        with open(file, "r") as f:
            csvreader = csv.reader(f)
            next(csvreader)
            img_path = (
                f"{user_id}.png"
                if os.path.exists(f"app/02-src-data/{user_id}.png")
                else ""
            )
            rows.append(
                [user_id] + [field.strip() for field in next(csvreader)] + [img_path]
            )

    with open("output.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "first_name", "last_name", "birthts", "img_path"])
        writer.writerows(rows)

    client = Minio(
        "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )
    client.fput_object("datalake", "output.csv", "output.csv")
    return {"success": True}


@app.get("/stats")
def get_stats(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    client = Minio(
        "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    where = get_where_clause(is_image_exists, min_age, max_age)

    with client.select_object_content(
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

    return {"average age": average_age}
