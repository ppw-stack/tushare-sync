from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from app.sync.sync_manager import sync_manager, SyncStatus
from app.sync import stock_sync
from typing import Optional

router = APIRouter()

class SyncResponse(BaseModel):
    job_id: str
    message: str

class JobStatusResponse(BaseModel):
    job_id: str
    data_type: str
    status: str
    message: str
    progress: float

@router.post("/sync/daily", response_model=SyncResponse)
async def trigger_daily_sync(
    background_tasks: BackgroundTasks,
    ts_code: Optional[str] = Query(None, description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD")
):
    job = sync_manager.create_job("daily")
    background_tasks.add_task(sync_manager.run_sync, job.job_id, "daily", ts_code=ts_code, start_date=start_date, end_date=end_date)

    return SyncResponse(job_id=job.job_id, message="日线数据同步任务已创建")

@router.post("/sync/trade_cal", response_model=SyncResponse)
async def trigger_trade_cal_sync(
    background_tasks: BackgroundTasks,
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD")
):
    job = sync_manager.create_job("trade_cal")
    background_tasks.add_task(sync_manager.run_sync, job.job_id, "trade_cal", start_date=start_date, end_date=end_date)

    return SyncResponse(job_id=job.job_id, message="交易日历同步任务已创建")

@router.post("/sync/stock_basic", response_model=SyncResponse)
async def trigger_stock_basic_sync(
    background_tasks: BackgroundTasks,
    is_his: Optional[str] = Query(None, description="是否包含历史数据 1 是 0 否")
):
    job = sync_manager.create_job("stock_basic")
    background_tasks.add_task(sync_manager.run_sync, job.job_id, "stock_basic", is_his=is_his)

    return SyncResponse(job_id=job.job_id, message="股票列表同步任务已创建")

@router.get("/sync/status/{job_id}", response_model=JobStatusResponse)
async def get_sync_status(job_id: str):
    job = sync_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    return JobStatusResponse(
        job_id=job.job_id,
        data_type=job.data_type,
        status=job.status.value,
        message=job.message,
        progress=job.progress
    )

@router.get("/health")
async def health():
    return {"status": "ok"}
