from inspect import signature
import json
from time import time,sleep
from collections import defaultdict
import hmac
from pathlib import Path
import csv
import base64
import hashlib
from urllib.parse import urlencode,quote
from copy import copy
from enum import Enum
from threading import Lock
from datetime import timezone, datetime, timedelta
import pytz
from typing import Any, Dict, List
from peewee import chunked

from vnpy.event import Event
from vnpy.trader.setting import bingx_account  #导入账户字典
from vnpy.event.engine import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.constant import (
    Interval,
    Status,
    Direction,
    Offset,
    Exchange
)
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    OrderType,
    OrderData,
    ContractData,
    Product,
    TickData,
    TradeData,
    HistoryRequest,
    BarData
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.utility import (delete_dr_data,remain_alpha,get_folder_path,load_json, save_json,get_local_datetime,extract_vt_symbol,TZ_INFO,remain_digit,GetFilePath,get_uuid,ACTIVE_STATUSES)
from vnpy.trader.database import database_manager
from vnpy.api.websocket import WebsocketClient
from vnpy.api.rest import Request, RestClient

recording_list = GetFilePath.recording_list

# REST API地址
REST_HOST: str = "https://open-api.bingx.com"

# Websocket API地址
WEBSOCKET_HOST: str = "wss://open-api-swap.bingx.com/swap-market"

# 委托类型映射
ORDERTYPE_VT2BINGX = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET"
}

ORDERTYPE_BINGX2VT = {v: k for k, v in ORDERTYPE_VT2BINGX.items()}

POSITIONSIDE_BINGX2VT = {
    "LONG":Direction.LONG,
    "SHORT":Direction.SHORT
}
POSITIONSIDE_VT2BINGX = {v: k for k, v in POSITIONSIDE_BINGX2VT.items()}

STATUS_BINGX2VT = {
    "NEW":Status.NOTTRADED,
    "FILLED":Status.ALLTRADED,
    "PARTIALLY_FILLED":Status.PARTTRADED,
    "EXPIRED":Status.REJECTED,
    "CANCELED":Status.CANCELLED,
}
# 买卖方向映射
DIRECTION_VT2BINGX = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL",
}
DIRECTION_BINGX2VT = {v: k for k, v in DIRECTION_VT2BINGX.items()}

# 多空反向映射
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}

# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
#------------------------------------------------------------------------------------------------- 
class BingxGateway(BaseGateway):
    """vn.py用于对接bingx的交易接口"""

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "代理地址": "",
        "代理端口": 0,
    }

    exchanges: Exchange = [Exchange.BINGX]
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, event_engine: EventEngine, gateway_name: str = "BINGX") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.ws_api: "BingxWebsocketApi" = BingxWebsocketApi(self)
        self.rest_api: "BingxRestApi" = BingxRestApi(self)
        self.orders: Dict[str, OrderData] = {}
        self.recording_list = [vt_symbol for vt_symbol in recording_list if extract_vt_symbol(vt_symbol)[2] == self.gateway_name  and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
        #查询历史数据合约列表
        self.history_contract = copy(self.recording_list)
        self.query_functions = [self.query_account,self.query_order,self.query_position]
        self.count = 0
    #------------------------------------------------------------------------------------------------- 
    def connect(self, log_account:dict = {}) -> None:
        """
        连接交易接口
        """
        if not log_account:
            log_account = bingx_account
        key: str = log_account["APIKey"]
        secret: str = log_account["PrivateKey"]
        proxy_host: str = log_account["代理地址"]
        proxy_port: int = log_account["代理端口"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, proxy_host, proxy_port)
        self.ws_api.connect(key, secret, proxy_host, proxy_port)
        self.init_query()
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.ws_api.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    #------------------------------------------------------------------------------------------------- 
    def query_position(self) -> None:
        """
        查询持仓
        """
        self.rest_api.query_position()
    #------------------------------------------------------------------------------------------------- 
    def query_order(self) -> None:
        """
        查询未成交委托
        """
        self.rest_api.query_order()
    #------------------------------------------------------------------------------------------------- 
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    #-------------------------------------------------------------------------------------------------   
    def query_history(self,event:Event):
        """
        查询合约历史数据
        """
        if len(self.history_contract) > 0:
            symbol,exchange,gateway_name = extract_vt_symbol(self.history_contract.pop(0))
            req = HistoryRequest(
                symbol = symbol,
                exchange = exchange,
                interval = Interval.MINUTE,
                start = datetime.now(TZ_INFO) - timedelta(days = 1),
                end = datetime.now(TZ_INFO),
                gateway_name = self.gateway_name
            )
            self.rest_api.query_history(req)
            self.rest_api.set_leverage(symbol)
    #------------------------------------------------------------------------------------------------- 
    def process_timer_event(self, event) -> None:
        """
        处理定时事件
        """
        function = self.query_functions.pop(0)
        function()
        self.query_functions.append(function)
        # 每隔30分钟发送一次延长listenkey请求
        self.count += 1
        if self.count < 1800:
            return
        self.count = 0
        self.rest_api.keep_listen_key()
    #------------------------------------------------------------------------------------------------- 
    def init_query(self):
        """
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TIMER, self.query_history)
    #------------------------------------------------------------------------------------------------- 
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.ws_api.stop()
#------------------------------------------------------------------------------------------------- 
class BingxRestApi(RestClient):
    """
    BINGX交易所REST API
    """
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, gateway: BingxGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ws_api: BingxWebsocketApi = self.gateway.ws_api

        # 保存用户登陆信息
        self.key: str = ""
        self.secret: str = ""
        # 确保生成的orderid不发生冲突
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        self.ticks:Dict[str, TickData] = self.gateway.ws_api.ticks
        self.account_date = None   #账户日期
        self.accounts_info:Dict[str,dict] = {}
        # 账户查询币种
        self.currencies = ["XBT","USDT"]
        # websocket令牌
        self.listen_key = ""
        # 用户自定义orderid与系统orderid映射
        self.orderid_map = {}
        # 系统orderid与委托单成交量映射
        self.trade_volume_map:Dict[str,float] = defaultdict(float)
    #------------------------------------------------------------------------------------------------- 
    def sign(self, request: Request) -> Request:
        """
        生成BINGX签名
        """
        # 获取鉴权类型并将其从data中删除
        security = request.data["security"]
        request.data.pop("security")
        if security == Security.NONE:
            request.data = None
            return request

        method = request.method
        params = request.params
        uri_path = request.path
        request_data = request.data
        if params:
            sorted_data = params
        elif request_data:
            sorted_data = request_data
        else:
            sorted_data = {}
        sorted_data["timestamp"] = int(time() *1000)
        sorted_data["recvWindow"] = 5000
        sorted_keys = sorted(sorted_data)
        params_str = "&".join(["{}={}".format(x, sorted_data[x]) for x in sorted_keys])
        request.path =  uri_path + "?" + params_str + f"&signature={get_sign(self.secret, params_str)}"
        request.data = {}
        if not request.headers:
            request.headers = {}
            request.headers["X-BX-APIKEY"] = self.key
        return request
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        连接REST服务器
        """
        self.key = key
        self.secret = secret.encode()
        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S"))
        )
        self.init(REST_HOST, proxy_host, proxy_port,gateway_name = self.gateway_name)
        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
        self.query_contract()
    #-------------------------------------------------------------------------------------------------   
    def set_leverage(self,symbol:str):
        """
        设置杠杆
        """
        sides = ["LONG","SHORT"]
        path: str = "/openApi/swap/v2/trade/leverage"
        for side in sides:
            data: dict = {
                "security": Security.SIGNED,
                "symbol":symbol,
                "side":side,
                "leverage":20,
                }
            self.add_request(
                method="POST",
                path=path,
                callback=self.on_leverage,
                data=data,
            )
    #-------------------------------------------------------------------------------------------------   
    def on_leverage(self,data:dict,request:dict):
        pass
    #-------------------------------------------------------------------------------------------------   
    def get_listen_key(self):
        """
        获取websocket私有令牌
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/user/auth/userDataStream"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_listen_key,
            data=data,
        )
    #------------------------------------------------------------------------------------------------- 
    def on_listen_key(self,data: dict, request: Request):
        """
        收到listen_key回报
        """
        self.listen_key:str = data["listenKey"]
    #-------------------------------------------------------------------------------------------------   
    def keep_listen_key(self):
        """
        延长websocket私有令牌
        """
        if not self.listen_key:
            return
        data: dict = {
            "security": Security.SIGNED,
            "listenKey":self.listen_key
            }
        path: str = "/openApi/user/auth/userDataStream"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_keep_listen_key,
            data=data,
        )
    #------------------------------------------------------------------------------------------------- 
    def on_keep_listen_key(self,data: dict, request: Request):
        """
        收到listen_key回报
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> None:
        """
        查询资金
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/user/balance"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )
    #------------------------------------------------------------------------------------------------- 
    def query_position(self) -> None:
        """
        查询持仓
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/user/positions"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data,
        )
    #------------------------------------------------------------------------------------------------- 
    def query_order(self) -> None:
        """
        查询未成交委托
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/openApi/swap/v2/trade/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data,
        )
    #------------------------------------------------------------------------------------------------- 
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        data: dict = {"security": Security.NONE}
        path: str = "/openApi/swap/v2/quote/contracts"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )
    #------------------------------------------------------------------------------------------------- 
    def get_traded_volume(self,symbol:str,system_id:str):
        """
        通过系统委托单号查询委托单成交量
        """
        data: dict = {
            "security": Security.SIGNED
            }
        params = {
            "symbol":symbol,
            "orderId":system_id
        }
        path: str = "/openApi/swap/v2/trade/order"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_traded_volume,
            data=data,
            params=params,
        )
    #------------------------------------------------------------------------------------------------- 
    def on_traded_volume(self,data: dict, request: Request) -> None:
        """
        """
        data = data["data"]["order"]
        self.trade_volume_map[data["orderId"]] = float(data["executedQty"])
    #------------------------------------------------------------------------------------------------- 
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + str(self.connect_time + self._new_order_id())

        # 推送提交中事件
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        data: dict = {
            "security": Security.SIGNED,
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINGX[req.direction],
            "price": float(req.price),
            "quantity": int(req.volume),
            "type": ORDERTYPE_VT2BINGX[req.type],
        }
        self.add_request(
            method="POST",
            path="/api/v1/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )
        return order.vt_orderid
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        必须用api生成的订单编号撤单
        """
        data: dict = {
            "security": Security.SIGNED,
            "symbol":req.symbol,
            "orderId":self.orderid_map.get(req.orderid,""),
            }
        path: str = "/openApi/swap/v2/trade/order"
        order: OrderData = self.gateway.get_order(req.orderid)
        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )
    #------------------------------------------------------------------------------------------------- 
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        资金查询回报
        """
        asset = data["data"]["balance"]
        account: AccountData = AccountData(
            accountid=asset["asset"] + "_" + self.gateway_name,
            balance=float(asset["balance"]),
            available = float(asset["availableMargin"]),
            position_profit = float(asset["unrealizedProfit"]),
            close_profit = float(asset["realisedProfit"]),
            datetime = datetime.now(TZ_INFO),
            gateway_name=self.gateway_name
        )
        account.frozen = account.balance - account.available
        if account.balance:
            self.gateway.on_account(account)
            #保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if  not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = GetFilePath().ctp_account_path.replace("ctp_account_1",self.gateway.account_file_name)
        for account_data in accounts_info:
            if not Path(account_path).exists(): # 如果文件不存在，需要写header
                with open(account_path, 'w',newline="") as f1:          #newline=""不自动换行
                    w1 = csv.DictWriter(f1, account_data.keys())
                    w1.writeheader()
                    w1.writerow(account_data)
            else: # 文件存在，不需要写header
                if self.account_date and self.account_date != account_date:        #一天写入一次账户信息         
                    with open(account_path,'a',newline="") as f1:                               #a二进制追加形式写入
                        w1 = csv.DictWriter(f1, account_data.keys())
                        w1.writerow(account_data)
        self.account_date = account_date         
    #------------------------------------------------------------------------------------------------- 
    def on_query_position(self, data: dict, request: Request) -> None:
        """
        持仓查询回报
        """
        for raw in data["data"]:
            position_1: PositionData = PositionData(
                symbol=raw["symbol"],
                exchange=Exchange.BINGX,
                direction=POSITIONSIDE_BINGX2VT[raw["positionSide"]],
                volume=float(raw["positionAmt"]),
                price=float(raw["avgPrice"]),
                pnl=float(raw["unrealizedProfit"]),
                gateway_name=self.gateway_name,
            )
            position_2 = PositionData(
                symbol=raw["symbol"],
                exchange=Exchange.BINGX,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[position_1.direction],
                volume = 0,
                price = 0,
                pnl = 0
            )
            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)
    #------------------------------------------------------------------------------------------------- 
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        活动委托查询回报
        """
        for raw in data["data"]["orders"]:
            volume = float(raw["origQty"])
            traded = float(raw["executedQty"])
            if traded < volume and raw["status"] != "CANCELLED":
                status = Status.PARTTRADED
            else:
                status=STATUS_BINGX2VT[raw["status"]]
            order: OrderData = OrderData(
                orderid=raw["orderId"],
                symbol=raw["symbol"],
                exchange=Exchange.BINGX,
                price=float(raw["price"]),
                volume=volume,
                type=ORDERTYPE_BINGX2VT[raw["type"]],
                direction=DIRECTION_BINGX2VT[raw["positionSide"]],
                traded=traded,
                status=status,
                datetime=get_local_datetime(raw["time"]),
                gateway_name=self.gateway_name,
            )
            if raw["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def on_query_contract(self, data: dict, request: Request):
        """
        合约信息查询回报
        """
        for raw in data["data"]:
            contract: ContractData = ContractData(
                symbol=raw["symbol"],
                exchange=Exchange.BINGX,
                name=raw["symbol"],
                price_tick=float("1e-{}".format(raw['pricePrecision'])),
                size=20,
                min_volume=float("1e-{}".format(raw['quantityPrecision'])),
                open_commission_ratio = raw["feeRate"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    #------------------------------------------------------------------------------------------------- 
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        order = request.extra
        system_id = data["order"]["orderId"]
        self.orderid_map[order.orderid] = system_id
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        msg: str = "委托失败，状态码：{0}，信息：{1}".format(status_code,request.response.text)
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order(self, status_code: str, request: Request) -> None:
        """
        委托撤单回报
        """
        data = request.response.json()
        code = data["code"]
        if int(code) == 100004:
            msg = data["msg"]
            if request.extra:
                order = request.extra
                order.status = Status.REJECTED
                self.gateway.on_order(order)
            msg = f"撤单失败，状态码：{code}，信息：{msg}"
            self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_failed(self, status_code: str, request: Request):
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history = []
        limit = 500
        start_time = int(datetime.timestamp(req.start) * 1000)
        end_time = int(datetime.timestamp(req.end) * 1000)
        time_consuming_start = time()
        while True:
            # 创建查询参数
            params = {
                "symbol":req.symbol,
                "interval":"1m",
                "startTime":start_time,
                "endTime":end_time
            }

            resp = self.request(
                "GET",
                "/openApi/swap/v2/quote/klines",
                data={"security": Security.NONE},
                params=params
            )
            # 如果请求失败则终止循环
            if not resp:
                msg = f"标的：{req.vt_symbol}获取历史数据失败"
                self.gateway.write_log(msg)
                break
            elif resp.status_code // 100 != 2:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    delete_dr_data(req.symbol,self.gateway_name)
                    msg = f"标的：{req.vt_symbol}获取历史数据为空，开始时间：{req.start}"
                    self.gateway.write_log(msg)
                    break
                buf = []
                for raw_data in data["data"]:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=get_local_datetime(raw_data["time"]),
                        interval=req.interval,
                        volume=float(raw_data["volume"]),
                        open_price=float(raw_data["open"]),
                        high_price=float(raw_data["high"]),
                        low_price=float(raw_data["low"]),
                        close_price=float(raw_data["close"]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)
                history.extend(buf)
            if len(history) >= limit:
                break
        if not history:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            return
            
        for bar_data in chunked(history, 10000):               #分批保存数据
            try:
                database_manager.save_bar_data(bar_data,False)      #保存数据到数据库  
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return    
        time_consuming_end =time()        
        query_time = round(time_consuming_end - time_consuming_start,3)
        msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime} ，结束时间： {history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)
#------------------------------------------------------------------------------------------------- 
class BingxWebsocketApi(WebsocketClient):
    """
    BINGX交易所Websocket接口
    """
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, gateway: BingxGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: BingxGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        #成交委托号
        self.trade_id:int = 0
        self.ws_connected:bool = False
        self.ping_count:int = 0
        self.put_data = {
            "depth5":self.on_depth,
            "trade":self.on_tick,
            "ACCOUNT_UPDATE":self.on_position,
            "ORDER_TRADE_UPDATE":self.on_order
        }
    #-------------------------------------------------------------------------------------------------
    def connect(
        self,
        api_key: str,
        api_secret: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        连接Websocket交易频道
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.listen_key = self.gateway.rest_api.listen_key
        while not self.listen_key:
            self.gateway.rest_api.get_listen_key()
            self.listen_key = self.gateway.rest_api.listen_key
            sleep(1)

        ws_host = f"{WEBSOCKET_HOST}?listenKey={self.listen_key}"
        self.init(ws_host, proxy_host, proxy_port,gateway_name = self.gateway_name)
        self.start()
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.ws_connected = True
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")
        for req in list(self.subscribed.values()):
            self.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.ws_connected = False
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket 连接断开")
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        # 等待ws连接成功后再订阅行情
        while not self.ws_connected:
            sleep(1)
        self.ticks[req.symbol] = TickData(
            symbol=req.symbol,
            name= req.symbol,
            exchange = req.exchange,
            gateway_name=self.gateway_name,
            datetime = datetime.now(TZ_INFO),
        )

        self.subscribed[req.symbol] = req
        # 订阅公共主题
        self.send_packet({'id': get_uuid(), 'reqType': 'sub', 'dataType':f"{req.symbol}@depth5"})
        self.send_packet({'id': get_uuid(), 'reqType': 'sub', 'dataType':f"{req.symbol}@trade"})
        #订阅私有主题
        self.send_packet({'id': get_uuid(), 'reqType': 'sub', 'dataType':"ORDER_TRADE_UPDATE"})
        self.send_packet({'id': get_uuid(), 'reqType': 'sub', 'dataType':"ACCOUNT_UPDATE"})
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet: Any) -> None:
        """
        推送数据回报
        """
        if packet == "Ping":
            self.send_packet("Pong")
            return
        if "dataType" in packet:
            type_ = packet["dataType"]
        elif "e" in packet:
            type_ = packet["e"]
        else:
            return
        if not type_:
            return
        if "@" in type_:
            type_ = type_.split("@")[1]
        channel = self.put_data.get(type_,None)
        if channel:
            channel(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_tick(self,packet:dict):
        """
        收到tick事件回报
        """
        data = packet["data"][0]
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.last_price = float(data["p"])
        tick.datetime = get_local_datetime(data["T"])
        self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_depth(self,packet:dict):
        """
        收到orderbook事件回报
        """
        symbol = packet["dataType"].split("@")[0]
        tick = self.ticks[symbol]
        data = packet["data"]
        bids = sorted(data["bids"],key=lambda x:x[0],reverse= True)
        asks = sorted(data["asks"],key=lambda x:x[0],reverse= False)
        for n,buf in enumerate(bids):
            tick.__setattr__(f"bid_price_{(n + 1)}", float(buf[0]))
            tick.__setattr__(f"bid_volume_{(n + 1)}", float(buf[1]))
        for n,buf in enumerate(asks):
            tick.__setattr__(f"ask_price_{(n + 1)}" , float(buf[0]))
            tick.__setattr__(f"ask_volume_{(n + 1)}", float(buf[1]))
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_position(self,packet: dict):
        """
        收到仓位事件回报
        """
        data = packet["P"]
        for pos_data in data:
            position_1: PositionData = PositionData(
                symbol=pos_data["s"],
                exchange=Exchange.BINGX,
                direction=POSITIONSIDE_BINGX2VT[pos_data["ps"]],
                volume=float(pos_data["pa"]),
                price=float(pos_data["ep"]),
                pnl=float(pos_data["up"]),
                gateway_name=self.gateway_name,
            )
            position_2 = PositionData(
                symbol=pos_data["s"],
                exchange=Exchange.BINGX,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[position_1.direction],
                volume = 0,
                price = 0,
                pnl = 0
            )
            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)
    #------------------------------------------------------------------------------------------------- 
    def on_order(self,packet: dict):
        """
        收到委托事件回报
        """
        data = packet["o"]
        # 用户委托单ID和系统委托单ID映射
        order_id = data["c"]
        system_id = data["i"]
        self.gateway.rest_api.get_traded_volume(data["s"],system_id)
        orderid_map = self.gateway.rest_api.orderid_map
        trade_volume_map = self.gateway.rest_api.trade_volume_map
        orderid_map[order_id] = system_id

        order: OrderData = OrderData(
            orderid=order_id,
            symbol=data["s"],
            exchange=Exchange.BINGX,
            price=float(data["p"]),
            volume=float(data["q"]),
            direction=DIRECTION_BINGX2VT[data["S"]],
            type = ORDERTYPE_BINGX2VT[data["o"]],
            status=STATUS_BINGX2VT[data["X"]],
            datetime=get_local_datetime(packet["E"]),
            gateway_name=self.gateway_name,
        )
        # 通过restapi获取委托单成交量
        trade_volume = trade_volume_map[system_id]
        order.traded = trade_volume
        self.gateway.on_order(order)
        if order.status not in [Status.NOTTRADED, Status.PARTTRADED]:
            if order_id in orderid_map:
                orderid_map.pop(order_id)
            if system_id in trade_volume_map:
                trade_volume_map.pop(system_id)

        if order.traded:
            self.trade_id += 1
            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BINGX,
                orderid=order.orderid,
                tradeid=self.trade_id,
                direction=DIRECTION_BINGX2VT[data["side"]],
                price=order.price,
                volume=order.traded,
                datetime=get_local_datetime(data["orderTime"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

def get_sign(api_secret, payload):

    signature = hmac.new(api_secret, payload.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    return signature
