import yaml
from pathlib import Path
from pydantic import BaseModel
from typing import Optional

class DatabaseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str

class TushareConfig(BaseModel):
    token: str
    http_url: str = "http://pro.tushare.cn/"

class AppConfig(BaseModel):
    host: str
    port: int

class Settings(BaseModel):
    database: DatabaseConfig
    tushare: TushareConfig
    app: AppConfig

def load_config(path: str = "config.yaml") -> Settings:
    config_path = Path(__file__).parent.parent / path
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return Settings(**data)

settings = load_config()
