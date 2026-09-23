import asyncio
import aiohttp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import io
import os
import time
from bingx_client import BingxClient
# ================= CONFIG =================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

ZONE_TF = "15m"              # Таймфрейм зон (настраиваемый)
ZONE_LIMIT = 1000             # Количество свечей
ATR_PERIOD = 20               # Период ATR
MIN_MOVE_MULT = 2             # Минимальное закрытие импульса за границей базы, в ATR
LOOKAHEAD = 4                # Импульс должен появиться сразу после базы
MAX_ZONES = 10                # Макс. зон на тип (supply/demand)
INVALIDATION_METHOD = "close" # "close" или "wick" для инвалидации
SCAN_INTERVAL_SEC = 300       # Интервал сканирования в секундах
MAX_CONCURRENT = 10
API_MIN_INTERVAL_SEC = 1.1      # Для свечей BingX: не чаще одного старта в секунду
CHART_CANDLES = 200           # Кол-во свечей на графике

MIN_ZONE_SCORE = 3.5
MAX_ZONE_ATR = 1.2             # Слишком широкую базу не обрезаем, а пропускаем
PRIOR_BARS = 6                # Проверка подхода к локальному экстремуму
# ==========================================

sent_signals = {}             # анти-дублирование
used_zones = {}               # для отслеживания использованных зон (одноразовые)

# ================= EXCHANGE =================

API_KEY = ''
API_SECRET = ''
bx = BingxClient(API_KEY, API_SECRET)
request_lock = asyncio.Lock()
next_request_at = 0.0


async def get_klines_limited(symbol, interval, limit):
    """Run the synchronous client off the event loop and pace API requests."""
    global next_request_at
    async with request_lock:
        now = time.monotonic()
        await asyncio.sleep(max(0.0, next_request_at - now))
        next_request_at = time.monotonic() + API_MIN_INTERVAL_SEC
    return await asyncio.to_thread(bx.get_klines, symbol, interval, limit)

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def interval_minutes(interval):
    unit = interval[-1]
    scale = {"m": 1, "h": 60, "d": 1440, "w": 10080}
    if unit not in scale:
        raise ValueError(f"Unsupported zone interval: {interval}")
    return int(interval[:-1]) * scale[unit]


def closed_candles(df, interval_minutes):
    """Leave only intervals whose closing time is already in the past."""
    if df.empty:
        return df.copy()
    times = pd.to_datetime(df["time"], utc=True)
    now = pd.Timestamp.now(tz="UTC")
    return df.loc[times + pd.Timedelta(minutes=interval_minutes) <= now].reset_index(drop=True)


def build_zone(df, i, zone_type, atr):
    """The source candle is the last countertrend or rejection candle."""
    candle = df.iloc[i]
    if zone_type == "demand":
        low, high = candle["low"], max(candle["open"], candle["close"])
    else:
        low, high = min(candle["open"], candle["close"]), candle["high"]
    if high <= low or high - low > MAX_ZONE_ATR * atr:
        return None
    return {
        "low": float(low), "high": float(high), "start_bar": i,
        "type": zone_type, "time": int(pd.Timestamp(candle["time"]).timestamp()),
    }


def is_bullish_pinbar_df(df, i):
    candle = df.iloc[i]
    body = abs(candle["close"] - candle["open"])
    span = candle["high"] - candle["low"]
    if span <= 0 or body > 0.35 * span or candle["close"] <= candle["open"]:
        return False
    lower = min(candle["open"], candle["close"]) - candle["low"]
    upper = candle["high"] - max(candle["open"], candle["close"])
    return lower >= 0.55 * span and lower >= 2 * upper


def is_bearish_pinbar_df(df, i):
    candle = df.iloc[i]
    body = abs(candle["close"] - candle["open"])
    span = candle["high"] - candle["low"]
    if span <= 0 or body > 0.35 * span or candle["close"] >= candle["open"]:
        return False
    upper = candle["high"] - max(candle["open"], candle["close"])
    lower = min(candle["open"], candle["close"]) - candle["low"]
    return upper >= 0.55 * span and upper >= 2 * lower


def has_prior_approach(df, i, zone, atr):
    """Demand is at the foot of a fall; supply is at the top of a rise."""
    before = df.iloc[i - PRIOR_BARS:i]
    first_close = before["close"].iloc[0]
    last_close = before["close"].iloc[-1]
    if zone["type"] == "demand":
        return (first_close - last_close >= 0.75 * atr
                and zone["low"] <= before["low"].min() + 0.3 * atr)
    return (last_close - first_close >= 0.75 * atr
            and zone["high"] >= before["high"].max() - 0.3 * atr)


def departure_score(df, i, zone, atr, kind):
    """A strong first candle and a close 2 ATR away within four bars."""
    first = df.iloc[i + 1]
    body = abs(first["close"] - first["open"])
    if body < 0.55 * atr:
        return None
    future = df.iloc[i + 1:min(len(df), i + LOOKAHEAD + 1)]
    if zone["type"] == "demand":
        if first["close"] <= first["open"] or first["close"] <= zone["high"] + 0.15 * atr:
            return None
        distances = future["close"] - zone["high"]
    else:
        if first["close"] >= first["open"] or first["close"] >= zone["low"] - 0.15 * atr:
            return None
        distances = zone["low"] - future["close"]
    confirmed = distances[distances >= MIN_MOVE_MULT * atr]
    if confirmed.empty:
        return None
    confirm_idx = int(confirmed.index[0])
    strength = float(distances.loc[confirm_idx] / atr)
    score = min(4.0, strength) + min(1.5, body / atr) + (1.0 if kind == "order" else 0.5)
    return confirm_idx, round(score, 2)


def is_zone_broken(df, zone):
    later = df.iloc[zone["confirmed_bar"] + 1:]
    if later.empty:
        return False
    if zone["type"] == "supply":
        values = later["close"] if INVALIDATION_METHOD == "close" else later["high"]
        return bool((values > zone["high"]).any())
    values = later["close"] if INVALIDATION_METHOD == "close" else later["low"]
    return bool((values < zone["low"]).any())


def was_retested(df, zone):
    """Keep only the first return; the newest bar may contain the live setup."""
    later = df.iloc[zone["confirmed_bar"] + 1:-1]
    if later.empty:
        return False
    if zone["type"] == "supply":
        return bool((later["high"] >= zone["low"]).any())
    return bool((later["low"] <= zone["high"]).any())


def get_nearest_zones(price, zones, n=2):
    def distance(zone):
        return max(zone["low"] - price, price - zone["high"], 0)
    return sorted(zones, key=distance)[:n]


def find_supply_demand_zones(df):
    if len(df) < ATR_PERIOD + PRIOR_BARS + 2:
        return [], []
    df = df.copy().reset_index(drop=True)
    prev_close = df["close"].shift(1)
    tr = np.maximum(df["high"] - df["low"],
                    np.maximum((df["high"] - prev_close).abs(),
                               (df["low"] - prev_close).abs()))
    # Shift keeps the candidate candle out of its own volatility baseline.
    df["atr"] = tr.rolling(ATR_PERIOD).mean().shift(1)
    zones = []
    for i in range(ATR_PERIOD + PRIOR_BARS, len(df) - 1):
        atr = df["atr"].iloc[i]
        if not np.isfinite(atr) or atr <= 0:
            continue
        candle = df.iloc[i]
        candidates = []
        bull_rejection = is_bullish_pinbar_df(df, i)
        bear_rejection = is_bearish_pinbar_df(df, i)
        if bull_rejection:
            candidates.append(("demand", "rejection"))
        elif candle["close"] < candle["open"] and not bear_rejection:
            candidates.append(("demand", "order"))
        if bear_rejection:
            candidates.append(("supply", "rejection"))
        elif candle["close"] > candle["open"] and not bull_rejection:
            candidates.append(("supply", "order"))
        for zone_type, kind in candidates:
            zone = build_zone(df, i, zone_type, atr)
            if zone is None or not has_prior_approach(df, i, zone, atr):
                continue
            departure = departure_score(df, i, zone, atr, kind)
            if departure is None:
                continue
            zone["confirmed_bar"], zone["score"] = departure
            zone["kind"] = kind
            if zone["score"] >= MIN_ZONE_SCORE and not is_zone_broken(df, zone) and not was_retested(df, zone):
                zones.append(zone)

    result = []
    for zone_type in ("supply", "demand"):
        chosen = []
        for zone in sorted((z for z in zones if z["type"] == zone_type),
                           key=lambda z: (z["score"], z["start_bar"]), reverse=True):
            if any(abs(zone["start_bar"] - old["start_bar"]) <= LOOKAHEAD
                   and min(zone["high"], old["high"]) > max(zone["low"], old["low"])
                   for old in chosen):
                continue
            chosen.append(zone)
            if len(chosen) == MAX_ZONES:
                break
        result.append(chosen)
    return result[0], result[1]

# ================= ПИНБАР НА 5M =================

def is_bearish_pinbar(candle):
    o = float(candle['open'])
    h = float(candle['high'])
    l = float(candle['low'])
    c = float(candle['close'])
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    if body > 0.3 * total_range:
        return False
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    if c >= o:
        return False
    if body == 0:
        return False
    if upper_wick >= body * 2 and lower_wick <= body * 0.5:
        return True
    return False

def is_bullish_pinbar(candle):
    o = float(candle['open'])
    h = float(candle['high'])
    l = float(candle['low'])
    c = float(candle['close'])
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    if body > 0.3 * total_range:
        return False
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    if c <= o:
        return False
    if body == 0:
        return False
    if lower_wick >= body * 2 and upper_wick <= body * 0.5:
        return True
    return False

def candle_retests_zone(candle, zone):
    """The wick touches the zone and the close stays near its near edge."""
    width = zone["high"] - zone["low"]
    if zone["type"] == "demand":
        return (candle["low"] <= zone["high"] and candle["high"] >= zone["low"]
                and zone["low"] <= candle["close"] <= zone["high"] + 0.25 * width)
    return (candle["high"] >= zone["low"] and candle["low"] <= zone["high"]
            and zone["low"] - 0.25 * width <= candle["close"] <= zone["high"])

def check_short_signal(symbol, df_5m, zones):
    last = df_5m.iloc[-1]

    timestamp = int(last["time"].timestamp())
    price = last["close"]

    if not is_bearish_pinbar_df(df_5m, len(df_5m) - 1):
        return None

    for zone in zones:
        if candle_retests_zone(last, zone) and (symbol, timestamp, "short", zone["time"]) not in sent_signals:
            return zone

    return None

def is_swing_high(df, i, left=2, right=2):
    if i < left or i + right >= len(df):
        return False
    return df['high'].iloc[i] == max(df['high'].iloc[i-left:i+right+1])

def is_swing_low(df, i, left=2, right=2):
    if i < left or i + right >= len(df):
        return False
    return df['low'].iloc[i] == min(df['low'].iloc[i-left:i+right+1])


def check_long_signal(symbol, df_5m, zones):
    last = df_5m.iloc[-1]

    timestamp = int(last["time"].timestamp())
    price = last["close"]

    if not is_bullish_pinbar_df(df_5m, len(df_5m) - 1):
        return None

    for zone in zones:
        if candle_retests_zone(last, zone) and (symbol, timestamp, "long", zone["time"]) not in sent_signals:
            return zone

    return None

# ================= ГЕНЕРАЦИЯ ГРАФИКА =================

def generate_chart(symbol, klines, supply_zones, demand_zones, signal_zone=None):
    # BingxClient already returns a sorted DataFrame with a `time` column.
    df_full = klines.copy().reset_index(drop=True)
    df_full['datetime'] = pd.to_datetime(df_full['time'])

    offset = max(0, len(df_full) - CHART_CANDLES)
    df = df_full.iloc[offset:].reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(20, 10))

    # Рисуем свечи
    for i in range(len(df)):
        o = df['open'][i]
        h = df['high'][i]
        l = df['low'][i]
        c = df['close'][i]
        color = 'green' if c > o else 'red'
        ax.add_patch(patches.Rectangle(
            (i - 0.2, min(o, c)),
            0.4,
            abs(c - o),
            facecolor=color,
            edgecolor=color
        ))
        ax.plot([i, i], [l, min(o, c)], color='black', linewidth=1)
        ax.plot([i, i], [max(o, c), h], color='black', linewidth=1)

    # Рисуем все SUPPLY зоны (красные)
    for zone in supply_zones:
        abs_start = zone['start_bar']
        rel_start = max(0, abs_start - offset)
        if rel_start >= len(df):
            continue
        x_end = len(df)
        color = 'red'
        ax.add_patch(patches.Rectangle(
            (rel_start, zone['low']),
            x_end - rel_start,
            zone['high'] - zone['low'],
            facecolor=color,
            alpha=0.2,
            edgecolor=color,
            linewidth=1
        ))
       

    # Рисуем все DEMAND зоны (синие)
    for zone in demand_zones:
        abs_start = zone['start_bar']
        rel_start = max(0, abs_start - offset)
        if rel_start >= len(df):
            continue
        x_end = len(df)
        color = 'blue'
        ax.add_patch(patches.Rectangle(
            (rel_start, zone['low']),
            x_end - rel_start,
            zone['high'] - zone['low'],
            facecolor=color,
            alpha=0.2,
            edgecolor=color,
            linewidth=1
        ))
        

    # Если есть сигнальная зона, выделяем её жирной обводкой
    if signal_zone:
        abs_start = signal_zone['start_bar']
        rel_start = max(0, abs_start - offset)
        if rel_start < len(df):
            color = 'red' if signal_zone['type'] == 'supply' else 'blue'
            ax.add_patch(patches.Rectangle(
                (rel_start, signal_zone['low']),
                len(df) - rel_start,
                signal_zone['high'] - signal_zone['low'],
                facecolor='none',
                edgecolor=color,
                linewidth=3,
                linestyle='-'
            ))

    ax.set_title(f"{symbol} TF:{ZONE_TF}")
    step = max(1, len(df) // 10)
    ax.set_xticks(range(0, len(df), step))
    ax.set_xticklabels(df['datetime'][::step].dt.strftime('%Y-%m-%d %H:%M'), rotation=45, ha='right')
    ax.grid(True)

    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()
    return buffer

# ================= TELEGRAM =================

async def send_telegram(image, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    data = aiohttp.FormData()
    data.add_field("chat_id", CHAT_ID)
    data.add_field("caption", caption)
    data.add_field("photo", image, filename="chart.png")
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            payload = await resp.json()
            if resp.status != 200 or not payload.get("ok"):
                raise RuntimeError(f"Telegram rejected signal: {resp.status} {payload}")
            return payload

# ================= ОБРАБОТКА СИМВОЛА =================

async def process_symbol(symbol, semaphore):
    async with semaphore:
        try:
            # 1. Получаем DataFrame свечей для зон (ZONE_TF)
            df_zone = closed_candles(await get_klines_limited(symbol, ZONE_TF, ZONE_LIMIT),
                                     interval_minutes(ZONE_TF))
            if df_zone.empty:
                return

            # 2. Вычисляем зоны (функция возвращает списки словарей)
            supply_zones, demand_zones = find_supply_demand_zones(df_zone)
           
            # 3. Текущая цена – последнее закрытие
            current_price = df_zone['close'].iloc[-1]

            # 4. Убираем уже использованные зоны до выбора ближайших.
            used = used_zones.get(symbol, set())
            supply_zones = get_nearest_zones(current_price,
                [z for z in supply_zones if (z['type'], z['time']) not in used], 2)
            demand_zones = get_nearest_zones(current_price,
                [z for z in demand_zones if (z['type'], z['time']) not in used], 2)

            if not supply_zones and not demand_zones:
                return

            # 5. Пинбар ищем только на закрытой пятиминутной свече.
            df_5m = closed_candles(await get_klines_limited(symbol, "5m", 3), 5)
            if df_5m.empty:
                return

            last_5m = df_5m.iloc[-1]
            c = last_5m['close']
            h = last_5m['high']
            l = last_5m['low']

            # 6. Проверяем касание зоны фитилём и форму свечи.
            short_zone = check_short_signal(symbol, df_5m, supply_zones)
            if short_zone:
                is_short = True
                zone = short_zone
            else:
                long_zone = check_long_signal(symbol, df_5m, demand_zones)
                if long_zone:
                    is_short = False
                    zone = long_zone
                else:
                    return
            
            # 7. Отправляем график; фиксируем сигнал только после успешного ответа.
            chart = generate_chart(symbol, df_zone, supply_zones, demand_zones, signal_zone=zone)

            signal_type = "Short" if is_short else "Long"
            stop = max(h, zone['high']) if is_short else min(l, zone['low'])
            side = "short" if is_short else "long"
            timestamp = int(last_5m["time"].timestamp())

            caption = (
                f"🚨 {symbol} {signal_type} Signal\n"
                f"Zone: {ZONE_TF} ({zone['kind']} block), retest: 5m pinbar\n"
                f"Zone: {zone['low']:.6f} - {zone['high']:.6f}\n"
                f"Reference price: {c:.6f}\n"
                f"Stop reference: {stop:.6f}"
            )
            await send_telegram(chart, caption)
            sent_signals[(symbol, timestamp, side, zone['time'])] = True
            used_zones.setdefault(symbol, set()).add((zone['type'], zone['time']))
            print(f"Signal найден: {symbol} {signal_type}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"{symbol} error: {e}")

# ================= ГЛАВНЫЙ ЦИКЛ =================

async def main_loop():
    symbols = await asyncio.to_thread(bx.get_all_tikers)
    if not symbols:
        raise RuntimeError("BingX did not return a contract list")
    print(f"Найдено {len(symbols)} монет")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    while True:
        print("Начинаем сканирование...")
        tasks = [process_symbol(symbol, semaphore) for symbol in symbols]
        await asyncio.gather(*tasks)
        print("Сканирование завершено\n")
        await asyncio.sleep(SCAN_INTERVAL_SEC)

if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Set BOT_TOKEN and CHAT_ID environment variables")
    asyncio.run(main_loop())
