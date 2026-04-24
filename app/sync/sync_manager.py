import asyncio
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)

class SyncStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

class SyncJob:
    def __init__(self, job_id: str, data_type: str, status: SyncStatus, 
                 message: str = "", progress: float = 0.0):
        self.job_id = job_id
        self.data_type = data_type
        self.status = status
        self.message = message
        self.progress = progress
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

class SyncManager:
    _instance: Optional["SyncManager"] = None
    
    def __init__(self):
        self.jobs: Dict[str, SyncJob] = {}
    
    @classmethod
    def get_instance(cls) -> "SyncManager":
        if cls._instance is None:
            cls._instance = SyncManager()
        return cls._instance
    
    def create_job(self, data_type: str) -> SyncJob:
        job_id = str(uuid.uuid4())[:8]
        job = SyncJob(job_id=job_id, data_type=data_type, status=SyncStatus.PENDING)
        self.jobs[job_id] = job
        logger.info(f"创建同步任务: {job_id}, 类型: {data_type}")
        return job
    
    def update_job(self, job_id: str, status: SyncStatus, message: str = "", progress: float = 0.0):
        if job_id in self.jobs:
            self.jobs[job_id].status = status
            self.jobs[job_id].message = message
            self.jobs[job_id].progress = progress
            self.jobs[job_id].updated_at = datetime.now()
    
    def get_job(self, job_id: str) -> Optional[SyncJob]:
        return self.jobs.get(job_id)
    
    async def run_sync(self, job_id: str, data_type: str, **kwargs):
        from app.sync import stock_sync
        self.update_job(job_id, SyncStatus.RUNNING, "同步中...")
        try:
            await stock_sync.sync_data(job_id, data_type, **kwargs)
            self.update_job(job_id, SyncStatus.SUCCESS, "同步完成", 1.0)
        except Exception as e:
            logger.error(f"同步失败: {e}")
            self.update_job(job_id, SyncStatus.FAILED, str(e))

sync_manager = SyncManager.get_instance()
