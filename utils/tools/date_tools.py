import random
from datetime import datetime, timedelta

def random_date(
        max_dias: int
    ) -> str:

    random_date = random.randint(0, max_dias)
    future_date = datetime.today() + timedelta(days = random_date)
    return future_date.strftime("%Y%m%d")


