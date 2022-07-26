import minio.error
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every

from app.data_processor import DataProcessor

app = FastAPI()
data_processor = DataProcessor()


@app.get("/data")
def get_data(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    try:
        users = data_processor.get_users(is_image_exists, min_age, max_age)
    except minio.error.S3Error as e:
        return {"success": False, "error": e.message}

    return {"success": True, "data": users}


@app.post("/data")
def post_data():
    data_processor.update_data()
    return {"success": True}


@app.get("/stats")
def get_stats(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    try:
        average_age = data_processor.get_average_age(is_image_exists, min_age, max_age)
    except minio.error.S3Error as e:
        return {"success": False, "error": e.message}

    return {"success": True, "data": average_age}


@app.on_event("startup")
@repeat_every(seconds=60 * 60)
def process_input_data_task() -> None:
    data_processor.update_data()
