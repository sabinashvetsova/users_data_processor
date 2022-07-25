from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every

from app.data_processor import DataProcessor

app = FastAPI()
data_processor = DataProcessor()


@app.get("/data")
def get_data(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    users = data_processor.get_users(is_image_exists, min_age, max_age)
    return {"users": users}


@app.post("/data")
def post_data():
    data_processor.update_data()
    return {"success": True}


@app.get("/stats")
def get_stats(is_image_exists: bool = None, min_age: int = None, max_age: int = None):
    average_age = data_processor.get_average_age(is_image_exists, min_age, max_age)
    return {"average_age": average_age}


@app.on_event("startup")
@repeat_every(seconds=30 * 1)
def process_input_data_task() -> None:
    data_processor.update_data()
