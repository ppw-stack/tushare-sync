from sqlalchemy import text
from app.database import AsyncSessionLocal
from datetime import datetime

async def ensure_partitions():
    """确保 stock_daily 的年分区存在（2010-current+1），VARCHAR分区键"""
    async with AsyncSessionLocal() as session:
        current_year = datetime.now().year
        for year in range(2010, current_year + 2):
            try:
                sql = f"""
                    CREATE TABLE IF NOT EXISTS stock_daily_{year}
                    PARTITION OF stock_daily
                    FOR VALUES FROM ('{year}0101') TO ('{year+1}0101')
                """
                await session.execute(text(sql))
                await session.commit()
            except Exception:
                await session.rollback()
