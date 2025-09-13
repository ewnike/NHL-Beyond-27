from sqlalchemy import text
from db_utils import get_db_engine

engine = get_db_engine()
with engine.begin() as conn:
    one = conn.execute(text("select 1")).scalar_one()
print("DB OK:", one == 1)
