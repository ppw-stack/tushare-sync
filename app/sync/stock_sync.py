import tushare as ts
import pandas as pd
import asyncio
import concurrent.futures
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from app.config import settings
from app.database import AsyncSessionLocal
from app.models.stock import StockBasic, StockDaily, TradeCal
from app.db_utils import ensure_partitions
import logging

logger = logging.getLogger(__name__)

# 全局线程池，用于执行阻塞的同步 HTTP 调用
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# 并发数
CONCURRENCY = 3
# API 超时（秒）
API_TIMEOUT = 30

def get_pro():
    pro = ts.pro_api(settings.tushare.token)
    pro._DataApi__http_url = settings.tushare.http_url
    return pro

def clean_value(val):
    """清理值，将NaN和None转为None"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return val

async def call_with_timeout(fn, timeout=API_TIMEOUT):
    """带超时的API调用，超时返回None"""
    try:
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(loop.run_in_executor(executor, fn), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"API调用超时({timeout}s): {fn.__name__ if hasattr(fn, '__name__') else str(fn)}")
        return None
    except Exception as e:
        logger.error(f"API调用异常: {e}")
        return None

async def sync_stock_basic(job_id: str):
    """同步股票基本信息 - 同步所有状态（L已上市 + D退市）"""
    from app.sync.sync_manager import sync_manager, SyncStatus

    logger.info(f"[{job_id}] 开始同步股票基本信息")

    pro = get_pro()
    total_count = 0

    statuses = ['L', 'D']

    async with AsyncSessionLocal() as session:
        for status in statuses:
            offset = 0
            limit = 5000
            status_name = "已上市" if status == 'L' else "已退市"

            while True:
                sync_manager.update_job(job_id, SyncStatus.RUNNING, f"同步{status_name}, 已同步 {total_count} 条", 0.1 + (total_count / 6000))

                def _call():
                    return pro.stock_basic(list_status=status, offset=offset, limit=limit)
                df = await call_with_timeout(_call)
                if df is None or len(df) == 0:
                    break

                for _, row in df.iterrows():
                    stmt = insert(StockBasic).values(
                        ts_code=clean_value(row['ts_code']),
                        symbol=clean_value(row.get('symbol')),
                        name=clean_value(row.get('name')),
                        area=clean_value(row.get('area')),
                        industry=clean_value(row.get('industry')),
                        cnspell=clean_value(row.get('cnspell')),
                        market=clean_value(row.get('market')),
                        list_date=clean_value(row.get('list_date')),
                        act_name=clean_value(row.get('act_name')),
                        act_ent_type=clean_value(row.get('act_ent_type'))
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['ts_code'],
                        set_={
                            'symbol': stmt.excluded.symbol,
                            'name': stmt.excluded.name,
                            'area': stmt.excluded.area,
                            'industry': stmt.excluded.industry,
                            'cnspell': stmt.excluded.cnspell,
                            'market': stmt.excluded.market,
                            'list_date': stmt.excluded.list_date,
                            'act_name': stmt.excluded.act_name,
                            'act_ent_type': stmt.excluded.act_ent_type
                        }
                    )
                    await session.execute(stmt)

                total_count += len(df)
                logger.info(f"[{job_id}] {status_name} offset={offset}, 本次{len(df)}条, 累计{total_count}条")

                if len(df) < limit:
                    break
                offset += limit

        await session.commit()

    sync_manager.update_job(job_id, SyncStatus.SUCCESS, f"同步完成，共 {total_count} 条", 1.0)
    logger.info(f"[{job_id}] 股票基本信息同步完成，共 {total_count} 条")
    return total_count

async def sync_trade_cal(job_id: str, start_date: str = None, end_date: str = None):
    """同步交易日历"""
    from app.sync.sync_manager import sync_manager, SyncStatus
    from datetime import datetime

    if not start_date:
        start_date = '20100101'
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')

    logger.info(f"[{job_id}] 开始同步交易日历 {start_date} - {end_date}")
    sync_manager.update_job(job_id, SyncStatus.RUNNING, "同步交易日历", 0.1)

    pro = get_pro()
    total_count = 0

    async with AsyncSessionLocal() as session:
        for exchange in ['SSE']:
            def _call():
                return pro.trade_cal(start_date=start_date, end_date=end_date, exchange=exchange)
            df = await call_with_timeout(_call)

            for _, row in df.iterrows():
                if row.get('is_open') == 1:
                    stmt = insert(TradeCal).values(
                        exchange=clean_value(row['exchange']),
                        cal_date=clean_value(row['cal_date']),
                        is_open=clean_value(row['is_open']),
                        pretrade_date=clean_value(row.get('pretrade_date'))
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['exchange', 'cal_date'],
                        set_={
                            'is_open': stmt.excluded.is_open,
                            'pretrade_date': stmt.excluded.pretrade_date
                        }
                    )
                    await session.execute(stmt)

            total_count += len(df)
            logger.info(f"[{job_id}] {exchange} {len(df)} 条")
            sync_manager.update_job(job_id, SyncStatus.RUNNING, f"已同步 {total_count} 条", 0.5 + (total_count / 12000))

        await session.commit()

    sync_manager.update_job(job_id, SyncStatus.SUCCESS, f"同步完成，共 {total_count} 条", 1.0)
    logger.info(f"[{job_id}] 交易日历同步完成，共 {total_count} 条")
    return total_count

async def _upsert_stock_daily(session, row):
    """UPSERT单条日线数据"""
    stmt = insert(StockDaily).values(
        ts_code=clean_value(row['ts_code']),
        trade_date=clean_value(row['trade_date']),
        open=clean_value(row['open']),
        high=clean_value(row['high']),
        low=clean_value(row['low']),
        close=clean_value(row['close']),
        pre_close=clean_value(row['pre_close']),
        vol=clean_value(row['vol']),
        amount=clean_value(row['amount']),
        change=clean_value(row.get('change')),
        pct_chg=clean_value(row.get('pct_chg'))
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=['ts_code', 'trade_date'],
        set_={
            'open': stmt.excluded.open,
            'high': stmt.excluded.high,
            'low': stmt.excluded.low,
            'close': stmt.excluded.close,
            'pre_close': stmt.excluded.pre_close,
            'vol': stmt.excluded.vol,
            'amount': stmt.excluded.amount,
            'change': stmt.excluded.change,
            'pct_chg': stmt.excluded.pct_chg
        }
    )
    await session.execute(stmt)

async def sync_daily(job_id: str, ts_code: str = None, start_date: str = None, end_date: str = None):
    """同步日线数据，支持按股票代码和日期范围筛选

    逻辑变更（按日期范围跨度决定同步策略）：
    - 日期跨度 > 60天：按股票维度同步（从 stock_basic 读取在交易股票，逐股票拉取整个时间范围）
    - 日期跨度 <= 60天：按交易日维度同步（遍历交易日列表，逐日拉取所有股票）
    - 没传 start_date/end_date，默认最近一个交易日
    - 每次都是完整同步，直接 UPSERT 覆盖
    - 带超时处理（30s），超时不阻塞
    - 并发数=3
    """
    from app.sync.sync_manager import sync_manager, SyncStatus
    from datetime import datetime

    logger.info(f"[{job_id}] 开始同步日线数据, ts_code={ts_code}, start={start_date}, end={end_date}")
    sync_manager.update_job(job_id, SyncStatus.RUNNING, "同步日线数据", 0.1)

    pro = get_pro()
    total_count = 0

    # 默认日期：最近一个交易日
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    if not start_date:
        start_date = '20240101'

    # 计算日期跨度
    date_span = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days

    async with AsyncSessionLocal() as session:
        # 确保分区存在
        await ensure_partitions()

        # 指定了 ts_code，单股票同步
        if ts_code:
            def _call():
                return pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            df = await call_with_timeout(_call)
            if df is None or len(df) == 0:
                sync_manager.update_job(job_id, SyncStatus.SUCCESS, "无数据", 1.0)
                return 0

            for _, row in df.iterrows():
                await _upsert_stock_daily(session, row)
                total_count += 1

            await session.commit()

        elif date_span > 60:
            # 长范围（>60天）：按股票维度同步，并发数=3
            logger.info(f"[{job_id}] 日期跨度 {date_span} 天 > 60，按股票维度同步，并发={CONCURRENCY}")

            def _call_basic():
                return pro.stock_basic(list_status='L', limit=5000)
            basic_df = await call_with_timeout(_call_basic)
            if basic_df is None or len(basic_df) == 0:
                sync_manager.update_job(job_id, SyncStatus.SUCCESS, "无股票数据", 1.0)
                return 0

            stock_codes = basic_df['ts_code'].tolist()
            total_stocks = len(stock_codes)
            logger.info(f"[{job_id}] 在交易股票共 {total_stocks} 只")

            async def fetch_one(code):
                """单只股票拉取，带超时"""
                def _inner():
                    return pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
                return await call_with_timeout(_inner)

            # 分批并发
            for batch_start in range(0, total_stocks, CONCURRENCY):
                batch = stock_codes[batch_start:batch_start + CONCURRENCY]
                completed = batch_start + len(batch)
                sync_manager.update_job(
                    job_id, SyncStatus.RUNNING,
                    f"进度 {completed}/{total_stocks}",
                    0.1 + 0.8 * (completed / total_stocks)
                )

                results = await asyncio.gather(*[fetch_one(code) for code in batch], return_exceptions=True)

                for code, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"[{job_id}] {code} 拉取异常: {result}")
                        continue
                    df = result
                    if df is None or len(df) == 0:
                        continue

                    for _, row in df.iterrows():
                        await _upsert_stock_daily(session, row)
                        total_count += 1

                await session.commit()
                logger.info(f"[{job_id}] 批次完成 {completed}/{total_stocks}")

        else:
            # 短范围（<=60天）：按交易日维度同步，并发数=3
            logger.info(f"[{job_id}] 日期跨度 {date_span} 天 <= 60，按交易日维度同步，并发={CONCURRENCY}")

            def _call_cal():
                return pro.trade_cal(start_date=start_date, end_date=end_date)
            cal_df = await call_with_timeout(_call_cal)
            trade_dates = sorted(cal_df[cal_df['is_open'] == 1]['cal_date'].tolist())

            async def fetch_day(d):
                """单日全市场拉取，带超时"""
                def _inner():
                    return pro.daily(trade_date=d)
                return await call_with_timeout(_inner)

            # 分批并发
            for batch_start in range(0, len(trade_dates), CONCURRENCY):
                batch = trade_dates[batch_start:batch_start + CONCURRENCY]
                completed = batch_start + len(batch)
                sync_manager.update_job(
                    job_id, SyncStatus.RUNNING,
                    f"进度 {completed}/{len(trade_dates)}",
                    0.1 + 0.8 * (completed / len(trade_dates))
                )

                results = await asyncio.gather(*[fetch_day(d) for d in batch], return_exceptions=True)

                for trade_date, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"[{job_id}] {trade_date} 拉取异常: {result}")
                        continue
                    df = result
                    if df is None or len(df) == 0:
                        continue

                    for _, row in df.iterrows():
                        await _upsert_stock_daily(session, row)
                        total_count += 1

                await session.commit()

    sync_manager.update_job(job_id, SyncStatus.SUCCESS, f"同步完成，共 {total_count} 条", 1.0)
    logger.info(f"[{job_id}] 日线数据同步完成，共 {total_count} 条")
    return total_count

async def sync_index_basic(job_id: str):
    """同步指数基本信息"""
    from app.sync.sync_manager import sync_manager, SyncStatus

    logger.info(f"[{job_id}] 开始同步指数基本信息")
    sync_manager.update_job(job_id, SyncStatus.RUNNING, "同步指数基本信息", 0.1)

    pro = get_pro()
    def _call():
        return pro.index_basic(limit=5000)
    df = await call_with_timeout(_call)

    sync_manager.update_job(job_id, SyncStatus.RUNNING, "获取完成", 1.0)
    logger.info(f"[{job_id}] 指数基本信息获取完成，共 {len(df)} 条")
    return len(df)

async def sync_data(job_id: str, data_type: str, **kwargs):
    if data_type == "stock_basic":
        return await sync_stock_basic(job_id)
    elif data_type == "daily":
        return await sync_daily(job_id, **kwargs)
    elif data_type == "trade_cal":
        return await sync_trade_cal(job_id, **kwargs)
    elif data_type == "index_basic":
        return await sync_index_basic(job_id)
    else:
        raise ValueError(f"不支持的数据类型: {data_type}")
