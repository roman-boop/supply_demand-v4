import time, hmac, hashlib, requests, json

from decimal import Decimal  # если не импортировано
from typing import Dict, List, Optional, Tuple
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple


class BingxClient:
    TESTNET_BASE_URL = "https://open-api-vst.bingx.com"
    REAL_BASE_URL = "https://open-api.bingx.com"

    def __init__(self, api_key, api_secret, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        self.BASE_URL = (
            self.TESTNET_BASE_URL
            if self.testnet
            else self.REAL_BASE_URL
        )

    def count_decimal_places(self, number: float) -> int:
        # Преобразуем число в строку с удалением лишних нулей после запятой
        s = str(number).rstrip('0')  
        if '.' in s:
            return len(s.split('.')[1])
        else:
            return 0
        
    def get_positions(self):
        """Получить все открытые позиции"""
        path = "/openApi/swap/v2/user/positions"
        data = self._request("GET", path, {"timestamp": int(time.time()*1000) + self.get_server_time_offset()})
        return data.get('data', []) if data.get('code') == 0 else []
    
    def _to_bingx_symbol(self, symbol: str) -> str:
        s = symbol.replace('-', '')
        s = s.replace('/', '')
        s = s.replace('USDT', '-USDT')
        return s

    def _sign(self, query: str) -> str:
        return hmac.new(self.api_secret.encode("utf-8"),
                        query.encode("utf-8"),
                        hashlib.sha256).hexdigest()

    def parseParam(self, paramsMap: dict) -> str:
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join(f"{k}={paramsMap[k]}" for k in sortedKeys)
        timestamp = str(int(time.time() * 1000))
        if paramsStr:
            return f"{paramsStr}&timestamp={timestamp}"
        else:
            return f"timestamp={timestamp}"



    def send_request(self, method: str, path: str, urlpa: str, payload: dict):
        sign = self._sign(urlpa)
        url = f"{self.BASE_URL}{path}?{urlpa}&signature={sign}"
        headers = {'X-BX-APIKEY': self.api_key}
        response = requests.request(method, url, headers=headers, data=payload)
        try:
            return response.json()
        except Exception as e:
            print("Ошибка при парсинге JSON:", e)
            print("Ответ сервера:", response.text)
            return None

    def set_max_leverage(self, symbol: str):
        path = '/openApi/swap/v2/quote/contracts'
        method = "GET"
        paramsStr = self.parseParam({})
        data = self.send_request(method, path, paramsStr, {})
        if not data or "data" not in data:
            print("Не удалось получить данные с BingX.")
            return data
        for item in data["data"]:
            if item["symbol"] == symbol:
                return item.get("maxLongLeverage")
        print(f"Символ {symbol} не найден.")
        return None

    def _request(self, method: str, path: str, params=None):
        if params is None:
            params = {}
        sorted_keys = sorted(params)
        query = "&".join([f"{k}={params[k]}" for k in sorted_keys])
        signature = self._sign(query)
        url = f"{self.BASE_URL}{path}?{query}&signature={signature}"
        headers = {"X-BX-APIKEY": self.api_key}
        r = requests.request(method, url, headers=headers)
        r.raise_for_status()
        return r.json()

    def _public_request(self, path: str, params=None, timeout: int = 10):
        url = f"{self.BASE_URL}{path}"
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def get_server_time_offset(self):
        url = f"{self.BASE_URL}/openApi/swap/v2/server/time"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        if data.get("code") == 0:
            server_time = int(data["data"]["serverTime"])
            local_time = int(time.time() * 1000)
            return server_time - local_time
        return 0
    def _parse_klines(self, rows: Any) -> pd.DataFrame:
        records: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                records.append(
                    {
                        "open_time": int(row.get("time") or row.get("openTime") or row.get("timestamp")),
                        "open": float(row.get("open")),
                        "high": float(row.get("high")),
                        "low": float(row.get("low")),
                        "close": float(row.get("close")),
                        "volume": float(row.get("volume", 0.0)),
                    }
                )
            elif isinstance(row, (list, tuple)) and len(row) >= 6:
                records.append(
                    {
                        "open_time": int(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
        if not records:
            raise RuntimeError("Empty kline payload")
        df = pd.DataFrame(records).sort_values("open_time").drop_duplicates(subset=["open_time"])
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.set_index("datetime")
        return df[["open_time", "open", "high", "low", "close", "volume"]].copy()
    
    
    import pandas as pd

    def get_klines(self, symbol, interval, limit: int = 1500) -> pd.DataFrame:
        payload = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "timestamp": int(time.time() * 1000)
        }

        data = self._public_request("/openApi/swap/v3/quote/klines", payload)
        if not isinstance(data, dict) or data.get("code") not in (0, "0", None):
            raise RuntimeError(f"BingX klines error for {symbol}: {data}")
        rows = data.get("data") or []

        if not rows:
            return pd.DataFrame()

        # BingX documents positional candles; some responses use named fields.
        df = self._parse_klines(rows).reset_index(drop=True)
        df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_localize(None)
        return df[["time", "open", "high", "low", "close", "volume"]]
    
    
    def get_all_tikers(self):
       
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/contracts"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        params = {}
        try:
        
            
            symbols = [item["symbol"] for item in data["data"]]
            return symbols
        except Exception as e:
            return None

    def get_mark_price(self, symbol=None):
        path = "/openApi/swap/v2/quote/premiumIndex"
        s = symbol or self.symbol
        params = {'symbol': s}

        try:
            data = self._public_request(path, params)
            if data.get('code') == 0 and 'data' in data:
                if isinstance(data['data'], list) and len(data['data']) > 0:
                    mark_price = data['data'][0].get('markPrice')
                    return float(mark_price) if mark_price is not None else None
                elif isinstance(data['data'], dict):
                    mark_price = data['data'].get('markPrice')
                    return float(mark_price) if mark_price is not None else None
           
        except Exception as e:
            return None


    def get_funding_rate(self, symbol=None):
        path = "/openApi/swap/v2/quote/premiumIndex"
        s = symbol or self.symbol
        params = {'symbol': s}

        try:
            data = self._public_request(path, params)
            if data.get('code') == 0 and 'data' in data:
                if isinstance(data['data'], list) and len(data['data']) > 0:
                    mark_price = data['data'][0].get('lastFundingRate')
                    return float(mark_price) if mark_price is not None else None
                elif isinstance(data['data'], dict):
                    mark_price = data['data'].get('lastFundingRate')
                    return float(mark_price) if mark_price is not None else None
        except Exception as e:
            return None



    def get_index_price(self, symbol=None):
        path = "/openApi/swap/v2/quote/premiumIndex"
        s = symbol or self.symbol
        params = {'symbol': s}
        try:
            data = self._public_request(path, params)
            if data.get('code') == 0 and 'data' in data:
                if isinstance(data['data'], list) and len(data['data']) > 0:
                    mark_price = data['data'][0].get('indexPrice')
                    return float(mark_price) if mark_price is not None else None
                elif isinstance(data['data'], dict):
                    mark_price = data['data'].get('indexPrice')
                    return float(mark_price) if mark_price is not None else None
            return None
        except Exception as e:
            return None
        
    def get_open_position(self, symbol: str, side: str):
        positions = self.get_positions()
        s = self._to_bingx_symbol(symbol)

        for pos in positions:
            if pos.get("symbol") != s:
                continue

            pos_side = pos.get("positionSide")
            if side == "long" and pos_side not in ("LONG", "BOTH"):
                continue
            if side == "short" and pos_side not in ("SHORT", "BOTH"):
                continue

            qty = float(pos.get("positionAmt", 0))
            if abs(qty) > 0:
                return pos

        return None

    
    def _generate_signature(self, params: Dict) -> str:
        """Из auto_sltp_manager.py: Подпись для запросов."""
        query_string = '&'.join([f"{key}={value}" for key, value in sorted(params.items())])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def get_open_orders(self, symbol: str) -> list:
        """Получить открытые ордера по символу."""
        endpoint = "/openApi/swap/v2/trade/openOrders"
        params = {'symbol': self._to_bingx_symbol(symbol), 'timestamp': int(time.time() * 1000) + self.get_server_time_offset()}
        data = self._request("GET", endpoint, params)
        return data.get('data', {}).get('orders', []) if data.get('code') == 0 else []

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Отмена одного ордера по orderId (твоя базовая функция, если нет — используй эту)."""
        endpoint = "/openApi/swap/v2/trade/order"
        params = {
            'symbol': self._to_bingx_symbol(symbol),
            'orderId': order_id,
            'timestamp': int(time.time() * 1000) + self.get_server_time_offset()
        }
        data = self._request("DELETE", endpoint, params)
        return data.get('code') == 0

    def cancel_existing_orders(self, symbol: str) -> bool:
        """НОВЫЙ МЕТОД: Полная отмена всех ордеров (по orderId + fallback)."""
        orders = self.get_open_orders(symbol)
        if not orders:
            print(f"[cancel_existing_orders] Нет ордеров для {symbol}")
            return True

        print(f"[cancel_existing_orders] Отменяем {len(orders)} ордеров для {symbol}")
        success_count = 0
        for order in orders:
            order_id = order.get('orderId')
            if order_id:
                if self.cancel_order(symbol, order_id):
                    success_count += 1
                    print(f"[cancel_existing_orders] Отменён {order_id}")
                time.sleep(0.4)  # Пауза для BingX

        # Fallback: если не все — отменяем все разом
        if success_count < len(orders):
            time.sleep(0.5)
            fallback_data = self._request("DELETE", "/openApi/swap/v2/trade/allOpenOrders", {'symbol': self._to_bingx_symbol(symbol), 'timestamp': int(time.time() * 1000) + self.get_server_time_offset()})
            if fallback_data.get('code') == 0:
                success_count += len(orders) - success_count
                print(f"[cancel_existing_orders] Fallback сработал")

        print(f"[cancel_existing_orders] Успешно: {success_count}/{len(orders)}")
        return success_count == len(orders)

    def get_existing_orders_info(self, symbol: str) -> tuple[bool, list]:
        """Из sl_tp_extender.py: Проверка SL и TP."""
        orders = self.get_open_orders(symbol)
        sl_exists = False
        tp_orders = []
        for order in orders:
            otype = order.get('type')
            if otype == 'STOP_MARKET':
                sl_exists = True
            elif otype == 'TAKE_PROFIT_MARKET':
                tp_orders.append({
                    "price": Decimal(str(order.get('stopPrice', 0))),
                    "quantity": Decimal(str(order.get('quantity', 0)))
                })
        return sl_exists, tp_orders

    def is_sl_exists(self, symbol):
        """Из sl_tp_extender.py: Проверка SL и TP."""
        orders = self.get_open_orders(symbol)
        sl_exists = False
        tp_orders = []
        for order in orders:
            otype = order.get('type')
            if otype == 'STOP_MARKET':
                sl_exists = True
            
        return sl_exists


   
        

    def place_market_order(self, side: str, qty: float, symbol: str = None, stop: float = None,
                           tp: float = None, pos_side_BOTH: bool = False, reduceOnly: bool = False):
        side_param = "BUY" if side == "long" else "SELL"
        s = self._to_bingx_symbol(symbol)
        pos_side = "LONG" if side == "long" else "SHORT"

        if pos_side_BOTH == True:
            pos_side = 'BOTH'

        params = {
            "symbol": s,
            "side": side_param,
            "positionSide": pos_side,
            "type": "MARKET",
            "quantity": qty,
            "recvWindow": 5000,
            "timeInForce": "GTC",
            
        }

        if reduceOnly and pos_side_BOTH:
            params["reduceOnly"] = "true"

        if stop is not None:
            stopLoss_param = {
                "type": "STOP_MARKET",
                "stopPrice": stop,
                "price": stop,
                "workingType": "MARK_PRICE"
            }
            params["stopLoss"] = json.dumps(stopLoss_param)
      
        if tp is not None:
            takeProfit_param = {
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": tp,
                "price": tp,
                "workingType": "MARK_PRICE"
            }
            params["takeProfit"] = json.dumps(takeProfit_param)

        timestamp = int(time.time() * 1000) 
        params["timestamp"] = timestamp

        return self._request("POST", "/openApi/swap/v2/trade/order", params)

    def set_sl(self, symbol: str, qty: float, side: str, stop, one_way_mode):

        qty_sl = qty 
        if one_way_mode:
            posside = 'BOTH'
        else:
            posside = 'LONG' if side == 'long' else 'SHORT'
        params = {
                "symbol": symbol,
                "side": "SELL" if side == "long" else "BUY",
                "positionSide": posside,
                "type": "STOP_MARKET",
                "stopPrice": stop,
                "price": stop,
                "quantity": qty_sl,
                "workingType": "MARK_PRICE",
                "timestamp": int(time.time() * 1000),
                "recvWindow": 5000
            }

        try:
            resp = self._request("POST", "/openApi/swap/v2/trade/order", params)
                
        except Exception as e:
            print(f"[SL2 ERROR] {e}")
        return resp
    def set_multiple_tp(self, symbol: str, qty: float, mark_price: float, side: str, tp_levels, both=False):
      
        precision = self.count_decimal_places(mark_price)

        if side == "short":
            tp_side = "BUY"
            pos_side = "SHORT"
        else:
            tp_side = "SELL"
            pos_side = "LONG"
        
        if both == True:
            pos_side = 'BOTH'
        answer = []
        qty_round = 0 if precision >= 3 else 2 if precision == 2 else 3 if precision == 1 else 4
        qty_tp = round(qty / len(tp_levels), qty_round)

        for tp in tp_levels:
            params = {
                "symbol": self._to_bingx_symbol(symbol),
                "side": tp_side,
                "positionSide": pos_side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": tp,
                "quantity": qty_tp,
                "timestamp": int(time.time()*1000) + self.get_server_time_offset(),
                "workingType": "MARK_PRICE"
            }
            try:
                resp = self._request("POST", "/openApi/swap/v2/trade/order", params)
                answer.append(resp)
                print(f"[TP] Установлен тейк-профит {tp}")
            except Exception as e:
                print("[TP ERROR]", e)
                answer.append({"code": 1, "msg": str(e)})

        return answer


    def set_trailing(self, symbol, side: str, qty: float, activation_price: float, priceRate: float, BOTH = False):
        pos_side = "LONG" if side =='long' else 'SHORT'
        if BOTH == True:
            pos_side = 'BOTH'
        params = {
            "symbol": symbol,
            "side": 'SELL' if side == 'long' else 'BUY',
            "positionSide": pos_side,
            "type": "TRAILING_TP_SL",
            "timestamp": int(time.time() * 1000) + self.get_server_time_offset(),
            "quantity": qty,
            "recvWindow": 5000,
            'workingType': 'CONTRACT_PRICE',
            'activationPrice': activation_price,
            "newClientOrderId": "",
            'priceRate': priceRate,
        }
        return self._request("POST", "/openApi/swap/v2/trade/order", params)


    def set_leverage_bx(self, symbol: str, side: str, leverage: int, one_way_mode = False):
        if one_way_mode == False:
            params = {
                "symbol": self._to_bingx_symbol(symbol),
                "side": side.upper(),
                "leverage": leverage,
                "timestamp": int(time.time() * 1000) + self.get_server_time_offset(),
            }



            resp1 = self._request("POST", "/openApi/swap/v2/trade/leverage", params)
        else:
            params = {
                "symbol": self._to_bingx_symbol(symbol),
                "side": 'BOTH',
                "leverage": leverage,
                "timestamp": int(time.time() * 1000) + self.get_server_time_offset(),
            }
            resp1 = self._request("POST", "/openApi/swap/v2/trade/leverage", params)
        return resp1
