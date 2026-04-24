from sqlalchemy import Column, String, Numeric, Integer
from app.models.base import Base

class StockBasic(Base):
    """股票基本信息表"""
    __tablename__ = "stock_basic"

    ts_code = Column(String(20), primary_key=True, comment="TS股票代码")
    symbol = Column(String(20), comment="股票代码")
    name = Column(String(100), comment="股票名称")
    area = Column(String(50), comment="地域")
    industry = Column(String(50), comment="所属行业")
    cnspell = Column(String(100), comment="拼音缩写")
    market = Column(String(20), comment="市场类型")
    list_date = Column(String(8), comment="上市日期")
    act_name = Column(String(100), comment="实控人名称")
    act_ent_type = Column(String(50), comment="实控人企业类型")

class TradeCal(Base):
    """交易日历表"""
    __tablename__ = "trade_cal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(10), primary_key=True, comment="交易所")
    cal_date = Column(String(8), primary_key=True, comment="交易日期 YYYYMMDD")
    is_open = Column(Integer, comment="是否交易 1=是 0=否")
    pretrade_date = Column(String(8), comment="前一交易日")

class StockDaily(Base):
    """日线行情表 - 对应tushare daily接口，RANGE分区(trade_date为VARCHAR YYYYMMDD)"""
    __tablename__ = "stock_daily"

    ts_code = Column(String(20), primary_key=True, comment="TS股票代码")
    trade_date = Column(String(8), primary_key=True, comment="交易日期 YYYYMMDD")
    open = Column(Numeric(10, 2), comment="开盘价")
    high = Column(Numeric(10, 2), comment="最高价")
    low = Column(Numeric(10, 2), comment="最低价")
    close = Column(Numeric(10, 2), comment="收盘价")
    pre_close = Column(Numeric(10, 2), comment="昨收价")
    vol = Column(Numeric(20, 2), comment="成交量")
    amount = Column(Numeric(20, 2), comment="成交额")
    change = Column(Numeric(10, 2), comment="涨跌额")
    pct_chg = Column(Numeric(10, 2), comment="涨跌幅")
