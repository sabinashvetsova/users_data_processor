from datetime import datetime

from dateutil.relativedelta import relativedelta


def get_where_clause(
    is_image_exists: bool = None, min_age: int = None, max_age: int = None
) -> str:
    where_conditions = []

    if is_image_exists is False:
        where_conditions.append("img_path = ''")

    if is_image_exists is True:
        where_conditions.append("img_path != ''")

    now = datetime.now()

    if min_age:
        min_birth_date = now - relativedelta(years=min_age)
        min_millisec = min_birth_date.timestamp() * 1000
        where_conditions.append(f"birthts <= {min_millisec}")

    if max_age:
        max_birth_date = now - relativedelta(years=max_age)
        max_millisec = max_birth_date.timestamp() * 1000
        where_conditions.append(f"birthts >= {max_millisec}")

    return "where " + " AND ".join(where_conditions) if where_conditions else ""
