#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BOT5 Ultra (REST-only, Integrated Upgrades + SELF-LEARNING) — v3
Cơ sở: bản v2 đã gửi trước đó, giữ nguyên logic & cấu trúc; bổ sung:
- TỰ HỌC (self-learning) từ chính *cảnh báo 1H*:
  * Ghi bảng alerts (coin, dir, score, price, atr, ts)
  * Sau H giờ đánh giá đúng/sai theo ATR-based outcome
  * Cập nhật EWMA success rate theo hướng (Long/Short) từng coin
  * Điều chỉnh confirm/alert/risk_factor trong phạm vi an toàn (bounded)
- Vẫn có Auto-Tune (dựa trên P&L trade) chạy song song.
- Regime/Session/Wick filters & 2-step confirm giữ nguyên.

Ghi chú:
- Bản này là paper-trade (không khớp lệnh thật). Đặt lệnh thật ngoài phạm vi bản này.
"""
import os, asyncio, time, csv, json, math, random, sqlite3, traceback, gzip, shutil
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import requests, pandas as pd, talib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== CONFIG =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8118417455:AAFyUEHeh-JzyUL9s51Ab7r69LuvVdcd364")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-4804203693")
USE_DYNAMIC_WATCHLIST = os.getenv("USE_DYNAMIC_WATCHLIST", "False") == "True"
STATIC_COINS = os.getenv("STATIC_COINS", "BTC,ETH,LTC,LINEA,SOL,BNB,ADA,PEPE").split(",")
DYN_TOP_N = int(os.getenv("DYN_TOP_N", "10"))
QUOTE = os.getenv("QUOTE", "USDT")
WATCHLIST_REFRESH_MIN = int(os.getenv("WATCHLIST_REFRESH_MIN", "20"))
TF_15M = "15m"; TF_1H = "1h"; TF_4H = "4h"
LIMIT = int(os.getenv("LIMIT", "200"))
SLEEP_BETWEEN_ROUNDS = int(os.getenv("SLEEP_BETWEEN_ROUNDS", "60"))
SHORT_DELAY = float(os.getenv("SHORT_DELAY", "0.2"))
COOLDOWN_BARS_1H = int(os.getenv("COOLDOWN_BARS_1H", "1"))
MAX_CONFIRMED_PER_ROUND = int(os.getenv("MAX_CONFIRMED_PER_ROUND", "3"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "4"))
MAX_PORTFOLIO_RISK = float(os.getenv("MAX_PORTFOLIO_RISK", "0.05"))
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))
BASE_RISK_PER_TRADE = float(os.getenv("BASE_RISK_PER_TRADE", "0.01"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT_SL = float(os.getenv("ATR_MULT_SL", "2.5"))
ATR_MULT_TRAIL = float(os.getenv("ATR_MULT_TRAIL", "2.0"))
TP1_RR = float(os.getenv("TP1_RR", "1.0"))
VOL_TARGET_ATR = float(os.getenv("VOL_TARGET_ATR", "0.01"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0004"))
SLIPPAGE_BPS = float(os.getenv("SLIPPAGE_BPS", "2.0"))
HEARTBEAT_MIN = int(os.getenv("HEARTBEAT_MIN", "0"))
ERROR_ALERT_COOLDOWN_SEC = int(os.getenv("ERROR_ALERT_COOLDOWN_SEC", "180"))
STATUS_FILE = os.getenv("STATUS_FILE", "bot5_ultra.status")
LOG_FILE = os.getenv("LOG_FILE", "signals_log.csv")
DB_FILE = os.getenv("DB_FILE", "bot5_ultra.sqlite3")
HOUSEKEEP_MIN = int(os.getenv("HOUSEKEEP_MIN", "15"))
LOG_MAX_MB = float(os.getenv("LOG_MAX_MB", "10"))
LOG_KEEP = int(os.getenv("LOG_KEEP", "7"))
LOG_GZIP = os.getenv("LOG_GZIP", "True") == "True"
DB_RETENTION_DAYS = int(os.getenv("DB_RETENTION_DAYS", "60"))
DB_MAX_MB = float(os.getenv("DB_MAX_MB", "200"))
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "archive")

# AutoTune (trên trades)
TUNE_ENABLED = os.getenv("TUNE_ENABLED", "True") == "True"
TUNE_FILE = os.getenv("TUNE_FILE", "bot5_ultra_tune.json")
TUNE_LOOKBACK_TRADES = int(os.getenv("TUNE_LOOKBACK_TRADES", "60"))
TUNE_MIN_TRADES = int(os.getenv("TUNE_MIN_TRADES", "20"))
TUNE_INTERVAL_MIN = int(os.getenv("TUNE_INTERVAL_MIN", "30"))
TUNE_CHANGE_LIMIT = float(os.getenv("TUNE_CHANGE_LIMIT", "0.15"))
TUNE_CONFIRM_BASE = float(os.getenv("TUNE_CONFIRM_BASE", "2.5"))
TUNE_ALERT_BASE = float(os.getenv("TUNE_ALERT_BASE", "1.2"))
TUNE_CONFIRM_MIN = float(os.getenv("TUNE_CONFIRM_MIN", "1.8"))
TUNE_CONFIRM_MAX = float(os.getenv("TUNE_CONFIRM_MAX", "3.5"))
TUNE_ALERT_MIN = float(os.getenv("TUNE_ALERT_MIN", "0.8"))
TUNE_ALERT_MAX = float(os.getenv("TUNE_ALERT_MAX", "2.0"))
RISK_FACTOR_MIN = float(os.getenv("RISK_FACTOR_MIN", "0.5"))
RISK_FACTOR_MAX = float(os.getenv("RISK_FACTOR_MAX", "1.5"))

# REST pacing & watcher
PACE_SEC = int(os.getenv("PACE_SEC", "20"))              # mỗi coin quét 1 lần/20s
PRICE_WATCH_SEC = int(os.getenv("PRICE_WATCH_SEC", "5")) # refresh giá cho vị thế mở

# New: chế độ
ALERT_ONLY = os.getenv("ALERT_ONLY", "False") == "True"   # chỉ cảnh báo, không mở vị thế
PAPER_TRADE = os.getenv("PAPER_TRADE", "True") == "True"  # bản này chỉ hỗ trợ paper-trade

BINANCE_API = "https://api.binance.com"
OKX_API = "https://www.okx.com"

# ====== SIÊU CHÍNH XÁC: REGIME & CONFIRM DYNAMICS ======
REGIME_ADX_MIN = float(os.getenv("REGIME_ADX_MIN", "20"))        # ADX thấp → sideway
HIGH_VOL_ATR_PCT = float(os.getenv("HIGH_VOL_ATR_PCT", "0.02"))  # ATR% > 2% coi là high-vol
CONFIRM_BUMP_ADX = float(os.getenv("CONFIRM_BUMP_ADX", "0.30"))  # tăng confirm khi ADX thấp
CONFIRM_BUMP_ATR = float(os.getenv("CONFIRM_BUMP_ATR", "0.20"))  # tăng confirm khi ATR% cao
CONFIRM_BUMP_WICK = float(os.getenv("CONFIRM_BUMP_WICK", "0.20"))# tăng confirm khi nến xấu
CONFIRM_BUMP_SESSION = float(os.getenv("CONFIRM_BUMP_SESSION", "0.20"))  # ngoài giờ vàng
ENABLE_SESSION_FILTER = os.getenv("ENABLE_SESSION_FILTER", "True") == "True"

# Giờ vàng theo UTC (ví dụ 12–20 UTC)
SESSION_UTC_WINDOWS = os.getenv("SESSION_UTC_WINDOWS", "12-20").split(",")

# Hai bước xác nhận (pending 1 nến 15m)
PENDING_CONFIRM_SEC = int(os.getenv("PENDING_CONFIRM_SEC", "900"))  # 900s = 15 phút

# ====== SELF-LEARNING CONFIG ======
LEARN_ENABLED = os.getenv("LEARN_ENABLED", "True") == "True"
LEARN_HORIZON_H = int(os.getenv("LEARN_HORIZON_H", "6"))          # đánh giá sau 6h
LEARN_TARGET_ATR = float(os.getenv("LEARN_TARGET_ATR", "0.5"))    # ngưỡng ±0.5×ATR
LEARN_ALPHA = float(os.getenv("LEARN_ALPHA", "0.2"))              # EWMA alpha
LEARN_INTERVAL_MIN = int(os.getenv("LEARN_INTERVAL_MIN", "10"))   # đánh giá định kỳ
LEARN_ADJ_CAP = float(os.getenv("LEARN_ADJ_CAP", "0.12"))         # trần thay đổi mỗi lần học
LEARN_SR_GOOD = float(os.getenv("LEARN_SR_GOOD", "0.60"))         # SR tốt
LEARN_SR_BAD  = float(os.getenv("LEARN_SR_BAD",  "0.40"))         # SR kém
LEARN_ALERT_RR = float(os.getenv("LEARN_ALERT_RR", "0.08"))       # đổi alert threshold (±)
LEARN_CONFIRM_RR = float(os.getenv("LEARN_CONFIRM_RR", "0.10"))   # đổi confirm threshold (±)
LEARN_RISK_RR = float(os.getenv("LEARN_RISK_RR", "0.10"))         # đổi risk_factor (±)
ALERT_RATE_LIMIT_MIN = int(os.getenv("ALERT_RATE_LIMIT_MIN", "10"))  # phút, tránh spam ghi alert trùng

# ----- HTTP SESSION with retries/backoff -----
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Bot5Ultra/REST"})
_retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retries, pool_connections=100, pool_maxsize=100)
SESSION.mount("https://", _adapter); SESSION.mount("http://", _adapter)

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or "YOUR_" in TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=12)
    except Exception as e: print("TG err:", e)

def safe_json(url: str, params=None):
    try:
        r = SESSION.get(url, params=params, timeout=12)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            if ra:
                try: time.sleep(float(ra))
                except: time.sleep(1.5)
                r = SESSION.get(url, params=params, timeout=12)
        r.raise_for_status()
        used = r.headers.get("X-MBX-USED-WEIGHT-1m")
        if used and used.isdigit() and int(used) >= 1100:
            time.sleep(0.5)
        return r.json()
    except Exception as e:
        print("GET fail:", e); return None

def ensure_csv_header():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["time","coin","price","tf15m_sig","tf1h_sig","tf4h_sig","score_15m","score_1h","score_4h","funding","oi","vote","notes"])

def write_status(state: Dict):
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f: json.dump(state, f, ensure_ascii=False, indent=2)
    except: pass

class DB:
    def __init__(self, path=DB_FILE):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        c = self.conn.cursor()
        # trades / positions (giữ nguyên)
        c.execute("""CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, coin TEXT, side TEXT, qty REAL, entry REAL, sl REAL, tp1 REAL, exit REAL, exit_reason TEXT, pnl REAL, fees REAL, run_id TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS positions(
            coin TEXT PRIMARY KEY, side TEXT, qty REAL, entry REAL, sl REAL, tp1 REAL, trail_step REAL, open_ts TEXT, run_id TEXT
        )""")
        # ===== Self-learning =====
        c.execute("""CREATE TABLE IF NOT EXISTS alerts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, coin TEXT, direction TEXT, score REAL,
            price REAL, atr REAL, horizon_h INTEGER,
            evaluated INTEGER DEFAULT 0, outcome INTEGER DEFAULT 0
        )""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_alerts_coin_ts ON alerts(coin, ts)""")
        c.execute("""CREATE TABLE IF NOT EXISTS learn_stats(
            coin TEXT, direction TEXT, ewma_sr REAL, count INTEGER,
            PRIMARY KEY(coin, direction)
        )""")
        self.conn.commit()

    # ---- trades/positions (giữ nguyên) ----
    def open_pos(self, coin, side, qty, entry, sl, tp1, trail_step, run_id):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?,?,?,?)",(coin,side,qty,entry,sl,tp1,trail_step,now_str(),run_id)); self.conn.commit()

    def close_pos(self, coin, exit_price, reason, fee_rate, run_id):
        c = self.conn.cursor(); c.execute("SELECT side, qty, entry, sl, tp1 FROM positions WHERE coin=?", (coin,)); row = c.fetchone()
        if not row: return None
        side, qty, entry, sl, tp1 = row; gross = (exit_price-entry)*qty if side=="Long" else (entry-exit_price)*qty
        fees = (abs(entry)*qty + abs(exit_price)*qty)*fee_rate; pnl = gross - fees
        c.execute("DELETE FROM positions WHERE coin=\"{}\"".format(coin))
        c.execute("INSERT INTO trades(ts,coin,side,qty,entry,sl,tp1,exit,exit_reason,pnl,fees,run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                  (now_str(), coin, side, qty, entry, sl, tp1, exit_price, reason, pnl, fees, run_id))
        self.conn.commit(); return {"coin": coin, "side": side, "qty": qty, "entry": entry, "exit": exit_price, "pnl": pnl, "fees": fees}

    def get_open_positions(self):
        c = self.conn.cursor()
        return {r[0]: {"side": r[1], "qty": r[2], "entry": r[3], "sl": r[4], "tp1": r[5], "trail_step": r[6]} for r in c.execute("SELECT coin, side, qty, entry, sl, tp1, trail_step FROM positions")}

    def update_sl_tp(self, coin: str, sl: Optional[float]=None, tp1: Optional[float]=None):
        c = self.conn.cursor()
        if sl is not None and tp1 is not None:
            c.execute("UPDATE positions SET sl=?, tp1=? WHERE coin=?", (sl, tp1, coin))
        elif sl is not None:
            c.execute("UPDATE positions SET sl=? WHERE coin=?", (sl, coin))
        elif tp1 is not None:
            c.execute("UPDATE positions SET tp1=? WHERE coin=?", (tp1, coin))
        self.conn.commit()

    def recent_trades(self, coin: str, limit: int) -> List[Tuple]:
        c = self.conn.cursor(); c.execute("SELECT ts, pnl FROM trades WHERE coin=? ORDER BY id DESC LIMIT ?", (coin, limit)); return c.fetchall()

    def prune_old(self, days: int):
        c = self.conn.cursor(); cutoff = (datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("DELETE FROM trades WHERE ts < ?", (cutoff,)); self.conn.commit(); c.execute("VACUUM"); self.conn.commit()

    # ---- self-learning ----
    def insert_alert(self, coin: str, direction: str, score: float, price: float, atr: float, horizon_h: int):
        c = self.conn.cursor()
        c.execute("INSERT INTO alerts(ts, coin, direction, score, price, atr, horizon_h, evaluated, outcome) VALUES (?,?,?,?,?,?,?,?,?)",
                  (now_str(), coin, direction, score, price, atr, horizon_h, 0, 0))
        self.conn.commit()

    def pending_alerts_to_eval(self) -> List[Tuple]:
        """Trả về các alert (id, ts, coin, dir, price, atr, horizon_h) quá hạn cần đánh giá."""
        c = self.conn.cursor()
        rows = []
        for r in c.execute("SELECT id, ts, coin, direction, price, atr, horizon_h FROM alerts WHERE evaluated=0"):
            # lọc ở Python theo thời gian
            try:
                ts = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            except:
                continue
            if datetime.now() >= ts + timedelta(hours=r[6]):
                rows.append(r)
        return rows

    def mark_alert(self, alert_id: int, outcome: int):
        c = self.conn.cursor()
        c.execute("UPDATE alerts SET evaluated=1, outcome=? WHERE id=?", (outcome, alert_id))
        self.conn.commit()

    def load_learn_stat(self, coin: str, direction: str) -> Tuple[float,int]:
        c = self.conn.cursor(); c.execute("SELECT ewma_sr, count FROM learn_stats WHERE coin=? AND direction=?", (coin, direction))
        row = c.fetchone()
        if not row: return 0.5, 0
        return float(row[0]), int(row[1])

    def upsert_learn_stat(self, coin: str, direction: str, ewma: float, count: int):
        c = self.conn.cursor()
        c.execute("INSERT INTO learn_stats(coin,direction,ewma_sr,count) VALUES (?,?,?,?) ON CONFLICT(coin,direction) DO UPDATE SET ewma_sr=?, count=?",
                  (coin, direction, ewma, count, ewma, count))
        self.conn.commit()


EX_INFO = {}

def fetch_exchange_info():
    j = safe_json(f"{BINANCE_API}/api/v3/exchangeInfo"); 
    if not j: return
    for s in j.get("symbols", []):
        if s.get("quoteAsset") != QUOTE: continue
        sym = s["baseAsset"]; tick=0.01; step=0.001
        for f in s.get("filters", []):
            if f["filterType"]=="PRICE_FILTER": tick=float(f["tickSize"])
            if f["filterType"]=="LOT_SIZE": step=float(f["stepSize"])
        EX_INFO[f"{sym}{QUOTE}"]={"tick":tick,"step":step}

def round_price(symbol, price): tick = EX_INFO.get(symbol,{"tick":0.01})["tick"]; return math.floor(price/tick)*tick
def round_qty(symbol, qty): step = EX_INFO.get(symbol,{"step":0.001})["step"]; return math.floor(qty/step)*step

def binance_klines(symbol: str, interval: str, limit: int=LIMIT) -> pd.DataFrame:
    j = safe_json(f"{BINANCE_API}/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    if not j or isinstance(j, dict): return pd.DataFrame()
    df = pd.DataFrame(j, columns=["t","o","h","l","c","v","ct","qv","nt","tb","tq","ig"])
    df["time"]=pd.to_datetime(df["t"], unit="ms"); 
    for col in ["o","h","l","c","v"]: df[col]=df[col].astype(float)
    return df.sort_values("time").reset_index(drop=True)[["time","o","h","l","c","v"]]

def okx_funding_oi(inst_id: str):
    fr = safe_json(f"{OKX_API}/api/v5/public/funding-rate", {"instId": inst_id})
    oi = safe_json(f"{OKX_API}/api/v5/public/open-interest", {"instId": inst_id})
    frv=oiv=None
    try: frv=float(fr["data"][0]["fundingRate"]) if fr and fr.get("data") else None
    except: pass
    try: oiv=float(oi["data"][0]["oi"]) if oi and oi.get("data") else None
    except: pass
    return frv, oiv

def binance_top_usdt_pairs(top_n: int=DYN_TOP_N):
    j = safe_json(f"{BINANCE_API}/api/v3/ticker/24hr")
    if not j or isinstance(j, dict): return []
    rows = [x for x in j if x.get("symbol","").endswith(QUOTE) and all(k not in x["symbol"] for k in ["UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT"])]
    rows.sort(key=lambda x: float(x.get("quoteVolume") or 0.0), reverse=True)
    return [r["symbol"].replace(QUOTE,"") for r in rows[:top_n]]

def calc_indicators(df: pd.DataFrame):
    if df.empty or len(df)<50: return None
    close=df["c"].values; high=df["h"].values; low=df["l"].values
    rsi=talib.RSI(close,14); macd,macdsig,_=talib.MACD(close,12,26,9)
    ema20=talib.EMA(close,20); ema50=talib.EMA(close,50); atr=talib.ATR(high,low,close,ATR_PERIOD)
    adx=talib.ADX(high,low,close,14)
    last_price=float(close[-1]); atr_val=float(atr[-1])
    atr_pct = (atr_val/last_price) if last_price>0 else 0.0
    return {"rsi":float(rsi[-1]),"macd":float(macd[-1]),"macdsig":float(macdsig[-1]),
            "ema20":float(ema20[-1]),"ema50":float(ema50[-1]),"atr":atr_val,"price":last_price,
            "adx": float(adx[-1]), "atr_pct": float(atr_pct)}

def detect_patterns(df: pd.DataFrame):
    """Trả về danh sách [(polarity, name)], polarity ∈ {'Bull','Bear'} để tránh thiên lệch Long."""
    if df.empty or len(df)<30: return []
    o,h,l,c=df["o"],df["h"],df["l"],df["c"]; out=[]
    pats={"Engulfing":talib.CDLENGULFING,"Hammer":talib.CDLHAMMER,"ShootingStar":talib.CDLSHOOTINGSTAR,"Doji":talib.CDLDOJI}
    for name,fn in pats.items():
        try:
            s=fn(o,h,l,c)
            if hasattr(s, "empty"):
                v=int(s.iloc[-1])
                if v>0: out.append(("Bull", name))
                elif v<0: out.append(("Bear", name))
        except: 
            pass
    return out

def _candle_quality(last_o, last_h, last_l, last_c):
    rng = max(1e-12, last_h - last_l)
    body = abs(last_c - last_o)
    upper = last_h - max(last_c, last_o)
    lower = min(last_c, last_o) - last_l
    return {"body_ratio": body / rng, "upper_ratio": upper / rng, "lower_ratio": lower / rng}

def score_frame(ind, pat, funding=None, oi=None, tf="1h"):
    if not ind: return 0.0, ["Thiếu dữ liệu"]
    rsi, macd, sig, ema20, ema50, price = ind["rsi"],ind["macd"],ind["macdsig"],ind["ema20"],ind["ema50"],ind["price"]
    adx, atr_pct = ind.get("adx"), ind.get("atr_pct")
    score=0.0; reasons=[f"Khung {tf}"]
    # Trend
    if price>ema20>ema50: score+=1.0; reasons.append("Trend↑: EMA20>EMA50")
    elif price<ema20<ema50: score-=1.0; reasons.append("Trend↓: EMA20<EMA50")
    # RSI
    reasons.append(f"RSI:{rsi:.2f}")
    if rsi<25: score+=0.8
    elif rsi>75: score-=0.8
    # MACD (đối xứng)
    if macd>sig: score+=0.5; reasons.append("MACD>Signal")
    else: score-=0.5; reasons.append("MACD<Signal")
    # Mẫu nến có phân cực
    if pat:
        reasons.append("Nến:"+", ".join([f"{pol}-{nm}" for pol,nm in pat]))
        for pol,nm in pat:
            if nm in ("Engulfing","Hammer"):
                score += 0.8 if pol=="Bull" else -0.8
            if nm=="ShootingStar":
                score += -0.8 if pol=="Bear" else 0.0
    # Funding / OI (để nguyên logic cũ)
    if tf=="1h":
        reasons.append(f"Funding:{funding} OI:{oi}")
        if funding is not None:
            if funding<-0.003: score+=1.0
            elif funding>0.003: score-=1.0
        if oi is not None:
            if oi<1e9: score+=0.3
            elif oi>3.5e9: score-=0.5
    # Regime info
    if adx is not None: reasons.append(f"ADX:{adx:.2f}")
    if atr_pct is not None: reasons.append(f"ATR%:{atr_pct*100:.2f}%")
    return float(score), reasons

def signal_from_score_static(score: float) -> str:
    if score>=2.5: return "✅ Xác nhận Long"
    if score<=-2.5: return "✅ Xác nhận Short"
    if abs(score)>=1.2: return "⚠️ Cảnh báo sớm: có dấu hiệu đảo chiều"
    return "⚖️ Trung tính"

def signal_from_score_dyn(score: float, confirm_th: float, alert_th: float) -> str:
    if score>=confirm_th: return "✅ Xác nhận Long"
    if score<=-confirm_th: return "✅ Xác nhận Short"
    if abs(score)>=alert_th: return "⚠️ Cảnh báo sớm: có dấu hiệu đảo chiều"
    return "⚖️ Trung tính"

def bias_from_score(score: float) -> int:
    if score>0: return 1
    if score<0: return -1
    return 0

def describe_reversal(new_bias: int, prev_bias: int) -> str:
    if new_bias==0: return "chưa rõ hướng"
    if prev_bias==0: return f"thiên về {'Long' if new_bias>0 else 'Short'}"
    if prev_bias>0 and new_bias<0: return "Long→Short"
    if prev_bias<0 and new_bias>0: return "Short→Long"
    return f"tiếp diễn {'Long' if new_bias>0 else 'Short'} (yếu)"

def vote_multi_tf(sig15: str, sig1h: str, sig4h: str) -> str:
    def sgn(s): return 1 if "Long" in s else (-1 if "Short" in s else 0)
    v4,v1,v15 = sgn(sig4h), sgn(sig1h), sgn(sig15); total=v4*2+v1*2+v15
    if (v4==1 and v1==-1) or (v4==-1 and v1==1): return "⚖️ Xung đột mạnh (1h↔4h)"
    if total>=4: return "✅ Đồng thuận Long (4h&1h)"
    if total<=-4: return "✅ Đồng thuận Short (4h&1h)"
    if abs(total)>=2: return "⚠️ Xu hướng nghiêng"
    return "⚖️ Chưa rõ ràng"

def volatility_adjusted_risk(atr_price: float, price: float) -> float:
    if not atr_price or atr_price<=0 or not price or price<=0: return BASE_RISK_PER_TRADE
    atr_pct=atr_price/price
    if atr_pct<=0: return BASE_RISK_PER_TRADE
    scale = VOL_TARGET_ATR/atr_pct
    return max(0.25*BASE_RISK_PER_TRADE, min(2.0*BASE_RISK_PER_TRADE, BASE_RISK_PER_TRADE*scale))

def propose_levels(price: float, atr: float, direction: str) -> Dict[str,float]:
    if not atr or atr<=0 or not price or price<=0 or not direction: return {}
    if direction=="Long":
        sl=price-ATR_MULT_SL*atr; tp1=price+(price-sl)*TP1_RR; trail=ATR_MULT_TRAIL*atr
    else:
        sl=price+ATR_MULT_SL*atr; tp1=price-(sl-price)*TP1_RR; trail=ATR_MULT_TRAIL*atr
    return {"entry":price,"sl":sl,"tp1":tp1,"trail_step":trail}

def position_size(balance: float, risk_per_trade: float, entry: float, sl: float) -> float:
    risk_amount=balance*risk_per_trade; stop=abs(entry-sl)
    if stop<=0: return 0.0
    return max(risk_amount/stop, 0.0)

def binance_price(symbol: str) -> Optional[float]:
    j = safe_json(f"{BINANCE_API}/api/v3/ticker/price", {"symbol": symbol})
    try:
        return float(j["price"]) if j and "price" in j else None
    except:
        return None

class Telegram:
    def __init__(self):
        self.offset=None; self.paused=False; self.heartbeat_min=HEARTBEAT_MIN; self.run_id=datetime.now().strftime("run%Y%m%d%H%M%S")
    def send(self,text): send_telegram(text)
    def poll(self):
        if not TELEGRAM_TOKEN or "YOUR_" in TELEGRAM_TOKEN: return []
        try:
            params={"timeout":0}; 
            if self.offset: params["offset"]=self.offset+1
            j=requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params=params, timeout=10).json()
        except: return []
        msgs=[]; 
        for upd in j.get("result", []):
            self.offset=upd["update_id"]; msg=upd.get("message") or upd.get("edited_message") or {}
            text=(msg.get("text") or "").strip()
            if text: msgs.append(text)
        return msgs

def _size_mb(p): 
    try: return os.path.getsize(p)/(1024*1024)
    except: return 0.0

def _ensure_archive_dir():
    if not os.path.exists(ARCHIVE_DIR): os.makedirs(ARCHIVE_DIR, exist_ok=True)

def rotate_log_if_needed():
    if not os.path.exists(LOG_FILE): return
    if _size_mb(LOG_FILE)<=LOG_MAX_MB: return
    _ensure_archive_dir(); ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    rotated=os.path.join(ARCHIVE_DIR, f"signals_log_{ts}.csv"); shutil.move(LOG_FILE, rotated)
    ensure_csv_header()
    if LOG_GZIP:
        gz=rotated+".gz"
        with open(rotated,"rb") as fi, gzip.open(gz,"wb") as fo: shutil.copyfileobj(fi,fo)
        os.remove(rotated)
    files=sorted([os.path.join(ARCHIVE_DIR,f) for f in os.listdir(ARCHIVE_DIR) if f.startswith("signals_log_")], reverse=True)
    for f in files[LOG_KEEP:]: 
        try: os.remove(f)
        except: pass

def prune_db_if_needed(db: 'DB'):
    db.prune_old(DB_RETENTION_DAYS)
    try:
        while _size_mb(DB_FILE)>DB_MAX_MB:
            c=db.conn.cursor(); c.execute("DELETE FROM trades WHERE id IN (SELECT id FROM trades ORDER BY ts ASC LIMIT 1000)"); db.conn.commit()
            c.execute("VACUUM"); db.conn.commit()
            if _size_mb(DB_FILE)<=DB_MAX_MB: break
    except Exception as e: print("prune_db_if_needed:", e)

def _load_tune(path: str):
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except: return {}

def _save_tune(path: str, data: Dict[str,Dict]):
    try:
        with open(path,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)
    except: pass

def _bounded_update(old: float, target: float, cap_ratio: float, lo: float, hi: float) -> float:
    delta=target-old; max_step=max(abs(old)*cap_ratio, 0.05)
    new=old + (max_step if delta>0 else -max_step) if abs(delta)>max_step else target
    return max(lo, min(hi, new))

def _calc_metrics(trades: List[Tuple]):
    if not trades: return 0.0,0.0,0.0
    pnls=[p for _,p in trades if p is not None]; 
    if not pnls: return 0.0,0.0,0.0
    n=len(pnls); wins=[p for p in pnls if p>0]; losses=[p for p in pnls if p<=0]
    wr=len(wins)/n if n>0 else 0.0; exp=sum(pnls)/n; sum_pos=sum(wins) if wins else 0.0; sum_neg=-sum(losses) if losses else 0.0
    pf=(sum_pos/sum_neg) if sum_neg>0 else (float('inf') if sum_pos>0 else 0.0)
    return wr, exp, pf

class Bot5Ultra:
    def __init__(self):
        ensure_csv_header(); fetch_exchange_info()
        self.db=DB(); self.tg=Telegram(); self.last_error_ts=0.0; self.last_watchlist_refresh=0.0
        self.coins=STATIC_COINS[:]
        self.states={c: {"last_sig_1h":None,"last_bias_1h":0,"bars":COOLDOWN_BARS_1H,
                         "pending_dir": None, "pending_ready_at": 0.0,
                         "last_alert_ts": 0.0} for c in self.coins}
        self.confirmed_sent=0; self.last_heartbeat_ts=0.0; self.last_housekeep_ts=0.0
        self.tune=_load_tune(TUNE_FILE) if TUNE_ENABLED else {}; self.tune_last_ts=0.0
        self.last_signal_hash = {}      # anti-spam signal theo giờ
        self.last_alert_hash = {}       # anti-spam ghi alert
        self.learn_last_ts = 0.0        # mốc thời gian lần học gần nhất
        for c in self.coins:
            self.tune.setdefault(c, {"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE,"risk_factor":1.0})

    def _parse_bool(self, s: str) -> Optional[bool]:
        sl = s.strip().lower()
        if sl in ("1","true","on","yes"): return True
        if sl in ("0","false","off","no"): return False
        return None

    def error_alert(self, err: str):
        now=time.time(); print("🛑", err)
        if now-self.last_error_ts>ERROR_ALERT_COOLDOWN_SEC: self.tg.send(f"🛑 *BOT5 Ultra lỗi*: ```\n{err}\n```"); self.last_error_ts=now

    def heartbeat_if_needed(self):
        if self.tg.heartbeat_min and (time.time()-self.last_heartbeat_ts)>self.tg.heartbeat_min*60:
            self.tg.send(f"⚙️ *BOT5 Ultra* vẫn đang chạy — {now_str()}"); self.last_heartbeat_ts=time.time()

    def housekeeping_if_needed(self, force=False):
        if not force and HOUSEKEEP_MIN<=0: return
        if force or (time.time()-self.last_housekeep_ts)>HOUSEKEEP_MIN*60:
            try:
                rotate_log_if_needed(); prune_db_if_needed(self.db); 
                if TUNE_ENABLED: self.auto_tune_if_needed(force=force)
                if LEARN_ENABLED: self.self_learn_if_needed(force=True)  # ép học trong housekeeping
            except Exception as e: self.error_alert(f"housekeeping: {e}")
            self.last_housekeep_ts=time.time()

    def auto_tune_if_needed(self, force=False):
        if (not force) and ((time.time()-self.tune_last_ts)<TUNE_INTERVAL_MIN*60): return
        self.tune_last_ts=time.time(); updated=False
        open_pos_keys=set(self.db.get_open_positions().keys())
        tune_set=sorted(set(self.coins) | open_pos_keys)
        for coin in tune_set:
            trades=self.db.recent_trades(coin, TUNE_LOOKBACK_TRADES)
            if len(trades)<TUNE_MIN_TRADES: continue
            wr,exp,pf=_calc_metrics(trades)
            conf_old=self.tune.setdefault(coin,{"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE,"risk_factor":1.0})["confirm"]
            alert_old=self.tune[coin]["alert"]; risk_old=self.tune[coin]["risk_factor"]
            if pf>1.3 and wr>=0.55:
                conf_tgt=max(TUNE_CONFIRM_MIN, conf_old*0.92); alert_tgt=max(TUNE_ALERT_MIN, alert_old*0.92); risk_tgt=min(RISK_FACTOR_MAX, risk_old*1.10)
            elif pf<0.9 or wr<=0.45:
                conf_tgt=min(TUNE_CONFIRM_MAX, conf_old*1.10); alert_tgt=min(TUNE_ALERT_MAX, alert_old*1.10); risk_tgt=max(RISK_FACTOR_MIN, risk_old*0.85)
            else:
                conf_tgt=conf_old + (TUNE_CONFIRM_BASE-conf_old)*0.2; alert_tgt=alert_old + (TUNE_ALERT_BASE-alert_old)*0.2; risk_tgt=risk_old + (1.0-risk_old)*0.2
            conf_new=_bounded_update(conf_old, conf_tgt, TUNE_CHANGE_LIMIT, TUNE_CONFIRM_MIN, TUNE_CONFIRM_MAX)
            alert_new=_bounded_update(alert_old, alert_tgt, TUNE_CHANGE_LIMIT, TUNE_ALERT_MIN, TUNE_ALERT_MAX)
            risk_step_cap=max(abs(risk_old)*TUNE_CHANGE_LIMIT, 0.05); risk_new=min(RISK_FACTOR_MAX, max(RISK_FACTOR_MIN, risk_old + max(-risk_step_cap, min(risk_step_cap, risk_tgt-risk_old))))
            if any(abs(x-y)>1e-6 for x,y in [(conf_new,conf_old),(alert_new,alert_old),(risk_new,risk_old)]):
                self.tune[coin].update({"confirm":conf_new,"alert":alert_new,"risk_factor":risk_new}); updated=True
                self.tg.send(f"🧠 Auto-Tune {coin}: confirm={conf_new:.2f}, alert={alert_new:.2f}, risk×{risk_new:.2f}")
        if updated: _save_tune(TUNE_FILE, self.tune)

    # ====== SELF-LEARNING CORE ======
    def _apply_learning_adjustments(self, coin: str, dir_: str, sr: float):
        """Điều chỉnh confirm/alert/risk_factor theo success rate (EWMA) cho từng hướng."""
        data=self.tune.setdefault(coin, {"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE,"risk_factor":1.0})
        conf_old, alert_old, risk_old = data["confirm"], data["alert"], data["risk_factor"]
        conf_tgt, alert_tgt, risk_tgt = conf_old, alert_old, risk_old

        # Nếu SR tốt -> đỡ khắt khe hơn & tăng risk nhẹ
        if sr >= LEARN_SR_GOOD:
            conf_tgt = max(TUNE_CONFIRM_MIN, conf_old*(1-LEARN_CONFIRM_RR))
            alert_tgt = max(TUNE_ALERT_MIN,  alert_old*(1-LEARN_ALERT_RR))
            risk_tgt  = min(RISK_FACTOR_MAX, risk_old*(1+LEARN_RISK_RR))
        # Nếu SR kém -> khắt khe hơn & giảm risk
        elif sr <= LEARN_SR_BAD:
            conf_tgt = min(TUNE_CONFIRM_MAX, conf_old*(1+LEARN_CONFIRM_RR))
            alert_tgt = min(TUNE_ALERT_MAX,  alert_old*(1+LEARN_ALERT_RR))
            risk_tgt  = max(RISK_FACTOR_MIN, risk_old*(1-LEARN_RISK_RR))
        # nếu trung tính thì giữ

        # Giới hạn mức thay đổi mỗi lần học
        conf_new = _bounded_update(conf_old, conf_tgt, LEARN_ADJ_CAP, TUNE_CONFIRM_MIN, TUNE_CONFIRM_MAX)
        alert_new = _bounded_update(alert_old, alert_tgt, LEARN_ADJ_CAP, TUNE_ALERT_MIN, TUNE_ALERT_MAX)
        # risk_factor cập nhật có nắp
        step_cap = max(abs(risk_old)*LEARN_ADJ_CAP, 0.03)
        delta = max(-step_cap, min(step_cap, risk_tgt-risk_old))
        risk_new = min(RISK_FACTOR_MAX, max(RISK_FACTOR_MIN, risk_old+delta))

        changed = any(abs(x-y)>1e-9 for x,y in [(conf_new,conf_old),(alert_new,alert_old),(risk_new,risk_old)])
        if changed:
            self.tune[coin].update({"confirm":conf_new,"alert":alert_new,"risk_factor":risk_new})
            _save_tune(TUNE_FILE, self.tune)
            self.tg.send(f"📈 Tự học {coin}-{dir_}: SR={sr:.2f} → confirm={conf_new:.2f}, alert={alert_new:.2f}, risk×{risk_new:.2f}")

    def self_learn_if_needed(self, force=False):
        if not LEARN_ENABLED: return
        if (not force) and (time.time()-self.learn_last_ts < LEARN_INTERVAL_MIN*60): return
        self.learn_last_ts = time.time()

        pend = self.db.pending_alerts_to_eval()
        if not pend: return
        # Đánh giá từng alert
        for alert_id, ts_str, coin, direction, price, atr, horizon_h in pend:
            try:
                # Load dữ liệu từ ts đến ts+h
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                sym = f"{coin}{QUOTE}"
                df = binance_klines(sym, TF_1H, LIMIT)
                if df.empty: 
                    continue
                # chọn các nến sau ts và trước ts+H
                mask = (df["time"] > pd.Timestamp(ts)) & (df["time"] <= pd.Timestamp(ts + timedelta(hours=horizon_h)))
                dfe = df.loc[mask]
                if dfe.empty:
                    continue
                close_H = float(dfe["c"].iloc[-1])
                thr = max(1e-9, (atr if atr and atr>0 else 0.0) * LEARN_TARGET_ATR)
                pnl = (close_H - price) if direction=="Long" else (price - close_H)
                # outcome: +1, -1, 0
                if pnl >= thr: outcome = 1
                elif pnl <= -thr: outcome = -1
                else: outcome = 0
                self.db.mark_alert(alert_id, outcome)

                # Cập nhật EWMA SR cho coin-dir
                sr_old, cnt = self.db.load_learn_stat(coin, direction)
                # convert outcome -> success=1 if 1, 0 if -1, 0.5 if neutral
                succ = 1.0 if outcome==1 else (0.0 if outcome==-1 else 0.5)
                sr_new = sr_old*(1-LEARN_ALPHA) + succ*LEARN_ALPHA
                self.db.upsert_learn_stat(coin, direction, sr_new, cnt+1)

                # Áp dụng điều chỉnh theo SR mới
                self._apply_learning_adjustments(coin, direction, sr_new)
            except Exception as e:
                self.error_alert(f"self_learn eval {coin}: {e}")

    def refresh_watchlist(self):
        if USE_DYNAMIC_WATCHLIST and ((time.time()-self.last_watchlist_refresh)>WATCHLIST_REFRESH_MIN*60 or not self.coins):
            dyn=binance_top_usdt_pairs(DYN_TOP_N)
            if dyn:
                self.coins=dyn
                self.states={c: {"last_sig_1h":None,"last_bias_1h":0,"bars":COOLDOWN_BARS_1H,
                                 "pending_dir": None, "pending_ready_at": 0.0,
                                 "last_alert_ts": 0.0} for c in self.coins}
                for c in self.coins: self.tune.setdefault(c, {"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE,"risk_factor":1.0})
                self.tg.send("📝 Watchlist động: "+", ".join(self.coins)); self.last_watchlist_refresh=time.time()

    def _cmd_help(self):
        self.tg.send(
            """🆘 Lệnh hỗ trợ:
/help — danh sách lệnh
/pause | /resume — tạm dừng/tiếp tục bot
/status — trạng thái (coins, positions, alert_only, paper, pace, tune)
/coins BTC,ETH,SOL — đặt danh mục đang quét
/risk 0.01 — đặt BASE_RISK_PER_TRADE
/heartbeat 5 — nhịp sống (phút); 0 = tắt
/pace 20 — PACE_SEC (giây/coin)
/set KEY VALUE — đặt biến toàn cục (hỗ trợ on/off/true/false/1/0)
/tune on | /tune off | /tune — Auto-Tune
/learn on | /learn off | /learn — Tự học từ cảnh báo
/gc — housekeeping + *ép* Auto-Tune & Learning chạy ngay
/close COIN — đóng vị thế COIN theo giá thị trường
/flatten — đóng *tất cả* vị thế
/alert on|off — bật/tắt ALERT_ONLY
/paper on|off — bật/tắt PAPER_TRADE (hiện chỉ paper)
"""
        )

    def handle_commands(self, texts: List[str]):
        for t in texts:
            low=t.lower()
            if low.startswith("/help"): self._cmd_help()
            elif low.startswith("/pause"): self.tg.paused=True; self.tg.send("⏸️ Bot đã *tạm dừng*. /resume để chạy tiếp.")
            elif low.startswith("/resume"): self.tg.paused=False; self.tg.send("▶️ Bot *tiếp tục chạy*.")
            elif low.startswith("/status"):
                open_pos=self.db.get_open_positions()
                parts=[f"{c}: c{self.tune.get(c,{}).get('confirm',TUNE_CONFIRM_BASE):.2f}/a{self.tune.get(c,{}).get('alert',TUNE_ALERT_BASE):.2f}/r×{self.tune.get(c,{}).get('risk_factor',1.0):.2f}" for c in self.coins]
                info = [
                    f"coins={','.join(self.coins)}",
                    f"paused={self.tg.paused}",
                    f"open={list(open_pos.keys())}",
                    f"ALERT_ONLY={ALERT_ONLY}",
                    f"PAPER_TRADE={PAPER_TRADE}",
                    f"PACE_SEC={PACE_SEC}",
                    f"TUNE_ENABLED={TUNE_ENABLED}",
                    f"LEARN_ENABLED={LEARN_ENABLED}",
                ]
                self.tg.send("📊 Trạng thái: "+" | ".join(info)+"\n"+" | ".join(parts))
            elif low.startswith("/coins"):
                parts=t.split(" ",1)
                if len(parts)==2 and parts[1].strip():
                    self.coins=[x.strip().upper() for x in parts[1].split(",") if x.strip()]
                    self.states={c: {"last_sig_1h":None,"last_bias_1h":0,"bars":COOLDOWN_BARS_1H,
                                     "pending_dir": None, "pending_ready_at": 0.0,
                                     "last_alert_ts": 0.0} for c in self.coins}
                    for c in self.coins: self.tune.setdefault(c, {"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE,"risk_factor":1.0})
                    self.tg.send("✅ Đã set COINS: "+", ".join(self.coins))
                else: self.tg.send("ℹ️ Dùng: `/coins BTC,ETH,SOL`")
            elif low.startswith("/risk"):
                parts=t.split(); 
                if len(parts)>=2:
                    try: globals()["BASE_RISK_PER_TRADE"]=float(parts[1]); self.tg.send(f"✅ BASE_RISK_PER_TRADE = {BASE_RISK_PER_TRADE}")
                    except: self.tg.send("❌ Sai số. Ví dụ: /risk 0.01")
            elif low.startswith("/heartbeat"):
                parts=t.split()
                if len(parts)>=2:
                    try: self.tg.heartbeat_min=int(parts[1]); self.tg.send(f"✅ HEARTBEAT_MIN = {self.tg.heartbeat_min} phút")
                    except: self.tg.send("❌ Sai số. Ví dụ: /heartbeat 5")
            elif low.startswith("/set"):
                parts=t.split()
                if len(parts)>=3:
                    key,val=parts[1], " ".join(parts[2:])
                    try:
                        if key in globals():
                            bv=self._parse_bool(val)
                            if bv is not None:
                                globals()[key]=bv
                            else:
                                try:
                                    if val.isdigit(): globals()[key]=int(val)
                                    else: globals()[key]=float(val)
                                except: globals()[key]=val
                            self.tg.send(f"✅ Set {key} = {globals()[key]}")
                        else: self.tg.send("❌ KEY không tồn tại.")
                    except Exception as e: self.tg.send(f"❌ Lỗi: {e}")
                else: self.tg.send("ℹ️ Dùng: /set KEY VALUE")
            elif low.startswith("/gc"): self.housekeeping_if_needed(force=True); self.tg.send("🧹 Dọn dẹp xong & đã *ép* Auto-Tune/Tự học.")
            elif low.startswith("/tune on"): globals()["TUNE_ENABLED"]=True; self.tg.send("🧠 Auto-Tune *ON*")
            elif low.startswith("/tune off"): globals()["TUNE_ENABLED"]=False; self.tg.send("🧠 Auto-Tune *OFF*")
            elif low.startswith("/tune"):
                if not TUNE_ENABLED: self.tg.send("ℹ️ Auto-Tune đang tắt. /tune on để bật.")
                else:
                    parts=[f"{c}: confirm={self.tune.get(c,{}).get('confirm',TUNE_CONFIRM_BASE):.2f}, alert={self.tune.get(c,{}).get('alert',TUNE_ALERT_BASE):.2f}, risk×{self.tune.get(c,{}).get('risk_factor',1.0):.2f}" for c in self.coins]
                    self.tg.send("🧠 Tune:\n" + "\n".join(parts))
            elif low.startswith("/learn on"): globals()["LEARN_ENABLED"]=True; self.tg.send("🧩 Self-Learning *ON*")
            elif low.startswith("/learn off"): globals()["LEARN_ENABLED"]=False; self.tg.send("🧩 Self-Learning *OFF*")
            elif low.startswith("/learn"):
                lines=[]
                for c in self.coins:
                    srL,cntL = self.db.load_learn_stat(c, "Long")
                    srS,cntS = self.db.load_learn_stat(c, "Short")
                    lines.append(f"{c}: SR_L={srL:.2f}({cntL}), SR_S={srS:.2f}({cntS})")
                self.tg.send("🧩 Self-Learning stats:\n"+"\n".join(lines))
            elif low.startswith("/pace"):
                parts=t.split()
                if len(parts)>=2:
                    try:
                        globals()["PACE_SEC"] = int(parts[1])
                        self.tg.send(f"✅ PACE_SEC = {PACE_SEC}s/coin")
                    except:
                        self.tg.send("❌ Sai số. Ví dụ: /pace 20")
                else:
                    self.tg.send(f"ℹ️ Đang đặt PACE_SEC = {PACE_SEC}s")
            elif low.startswith("/alert"):
                parts=t.split()
                if len(parts)>=2:
                    bv=self._parse_bool(parts[1])
                    if bv is None: self.tg.send("❌ Dùng: /alert on|off"); continue
                    globals()["ALERT_ONLY"]=bv; self.tg.send(f"✅ ALERT_ONLY = {ALERT_ONLY}")
                else:
                    self.tg.send(f"ℹ️ ALERT_ONLY = {ALERT_ONLY}")
            elif low.startswith("/paper"):
                parts=t.split()
                if len(parts)>=2:
                    bv=self._parse_bool(parts[1])
                    if bv is None: self.tg.send("❌ Dùng: /paper on|off"); continue
                    globals()["PAPER_TRADE"]=bv; self.tg.send(f"✅ PAPER_TRADE = {PAPER_TRADE} (bản này chỉ paper)")
                else:
                    self.tg.send(f"ℹ️ PAPER_TRADE = {PAPER_TRADE}")
    def fetch_frames(self, coin: str):
        symbol=f"{coin}{QUOTE}"
        df1h=binance_klines(symbol, TF_1H, LIMIT); df15=binance_klines(symbol, TF_15M, LIMIT); df4h=binance_klines(symbol, TF_4H, LIMIT)
        return df15, df1h, df4h

    def update_positions_with_price(self, coin: str, price: float):
        try:
            pos=self.db.get_open_positions().get(coin)
            if not pos or price is None or not (price==price):  # NaN guard
                return
            side, qty, entry, sl, tp1, trail = pos["side"], pos["qty"], pos["entry"], pos["sl"], pos["tp1"], pos["trail_step"]
            sl_changed=False; tp_changed=False
            if side=="Long":
                # Khi chạm TP1, dời SL về entry
                if price>=tp1 and sl<entry:
                    sl=max(sl, entry); sl_changed=True; tp_changed=True
                # trailing
                new_sl=max(sl, price-trail)
                if new_sl>sl:
                    sl=new_sl; sl_changed=True
                # check SL hit
                if price<=sl:
                    self.db.close_pos(coin, price*(1 - SLIPPAGE_BPS/10000.0), "SL/Trail", FEE_RATE, self.tg.run_id)
                    self.tg.send(f"✅ Đóng {coin} Long @ {price:.4f} (SL/Trail)"); return
            else:
                if price<=tp1 and sl>entry:
                    sl=min(sl, entry); sl_changed=True; tp_changed=True
                new_sl=min(sl, price+trail)
                if new_sl<sl:
                    sl=new_sl; sl_changed=True
                if price>=sl:
                    self.db.close_pos(coin, price*(1 + SLIPPAGE_BPS/10000.0), "SL/Trail", FEE_RATE, self.tg.run_id)
                    self.tg.send(f"✅ Đóng {coin} Short @ {price:.4f} (SL/Trail)"); return
            if sl_changed or tp_changed:
                self.db.update_sl_tp(coin, sl if sl_changed else None, entry if tp_changed else None)
        except Exception as e: self.error_alert(f"update_positions_with_price: {e}")

    def _session_confirm_bump(self) -> float:
        """Nếu ngoài giờ vàng (UTC), tăng ngưỡng confirm."""
        if not ENABLE_SESSION_FILTER: return 0.0
        try:
            h = datetime.utcnow().hour
            for win in SESSION_UTC_WINDOWS:
                if "-" not in win: continue
                a,b = win.split("-")
                a=int(a); b=int(b)
                if a<=h<b:  # nằm trong 1 khung
                    return 0.0
            return CONFIRM_BUMP_SESSION  # ngoài tất cả khung
        except:
            return 0.0

    def _maybe_log_alert(self, coin: str, direction: Optional[str], sc1h: float, alert_dyn: float, price: Optional[float], atr: Optional[float]):
        """Ghi lại *cảnh báo 1H* để sau này tự học. Có rate limit chống trùng."""
        if direction is None: return
        if price is None or atr is None or atr<=0: return
        if abs(sc1h) < alert_dyn: return
        # rate limit per coin (tránh spam)
        st = self.states.setdefault(coin, {"last_alert_ts": 0.0})
        if time.time() - st.get("last_alert_ts", 0.0) < ALERT_RATE_LIMIT_MIN*60:
            return
        # chống trùng theo chữ ký giờ
        key = f"{coin}|{direction}|{int(datetime.now().timestamp()//3600)}"
        if self.last_alert_hash.get(coin) == key:
            return
        self.last_alert_hash[coin] = key
        # ghi
        self.db.insert_alert(coin, direction, sc1h, float(price), float(atr), LEARN_HORIZON_H)
        st["last_alert_ts"] = time.time()

    def one_coin_round(self, coin: str) -> Optional[float]:
        """Quét 1 coin bằng REST, trả về 'price' dùng để cập nhật vị thế."""
        t0 = time.time()
        try:
            symbol=f"{coin}{QUOTE}"; inst_okx=f"{coin}-{QUOTE}-SWAP"
            df15,df1h,df4h=self.fetch_frames(coin)
            if df1h.empty or df4h.empty:
                print(f"⚠️ Thiếu dữ liệu {coin}")
                return None
            funding,oi=okx_funding_oi(inst_okx)
            ind15,ind1h,ind4h = calc_indicators(df15), calc_indicators(df1h), calc_indicators(df4h)
            price = ind1h["price"] if ind1h else None
            pat15,pat1h,pat4h = detect_patterns(df15), detect_patterns(df1h), detect_patterns(df4h)
            sc15,_=score_frame(ind15,pat15,tf="15m"); sc1h,rs1h=score_frame(ind1h,pat1h,funding,oi,tf="1h"); sc4h,_=score_frame(ind4h,pat4h,tf="4h")

            # === Dynamic thresholds (Regime/Wick/Session) ===
            tune_c=self.tune.get(coin, {"confirm":TUNE_CONFIRM_BASE,"alert":TUNE_ALERT_BASE})
            confirm_dyn = tune_c["confirm"]
            alert_dyn = tune_c["alert"]

            # ADX bump (sideway) & ATR% bump (biến động quá cao)
            adx = ind1h.get("adx") if ind1h else None
            if adx is not None and adx < REGIME_ADX_MIN:
                confirm_dyn += CONFIRM_BUMP_ADX
            atr_pct = ind1h.get("atr_pct") if ind1h else None
            if atr_pct is not None and atr_pct > HIGH_VOL_ATR_PCT:
                confirm_dyn += CONFIRM_BUMP_ATR

            # Wick/Body filter (nến xấu → tăng confirm)
            if not df1h.empty:
                last = df1h.iloc[-1]
                cq = _candle_quality(last["o"], last["h"], last["l"], last["c"])
                if cq["body_ratio"] < 0.5 or cq["upper_ratio"] > 0.5 or cq["lower_ratio"] > 0.5:
                    confirm_dyn += CONFIRM_BUMP_WICK

            # Session filter
            confirm_dyn += self._session_confirm_bump()

            # Dùng thresholds động
            s15=signal_from_score_static(sc15)
            s1h=signal_from_score_dyn(sc1h, confirm_dyn, alert_dyn)
            s4h=signal_from_score_static(sc4h)

            st=self.states.setdefault(coin, {"last_sig_1h":None,"last_bias_1h":0,"bars":COOLDOWN_BARS_1H,
                                             "pending_dir": None, "pending_ready_at": 0.0,
                                             "last_alert_ts": 0.0})
            new_bias=bias_from_score(sc1h); prev_bias=st.get("last_bias_1h",0)
            s1h_pretty=s1h + (f" — *{describe_reversal(new_bias, prev_bias)}*" if "⚠️" in s1h else "")
            vote=vote_multi_tf(s15,s1h,s4h)

            open_pos=self.db.get_open_positions()
            if len(open_pos)>=MAX_CONCURRENT_POSITIONS:
                note="🧯 Max concurrent positions"
            else:
                direction="Long" if ("Đồng thuận Long" in vote or "Xác nhận Long" in s1h) else ("Short" if ("Đồng thuận Short" in vote or "Xác nhận Short" in s1h) else None)
                lvls=propose_levels(price, ind1h.get("atr") if ind1h else None, direction) if direction else {}
                atr=ind1h.get("atr") if ind1h else None
                risk_pt=volatility_adjusted_risk(atr, price) * self.tune.get(coin,{}).get("risk_factor",1.0)
                current_risk=len(open_pos)*BASE_RISK_PER_TRADE
                note="🧯 Max portfolio risk" if current_risk + risk_pt > MAX_PORTFOLIO_RISK else ""

            duration_ms = int((time.time() - t0)*1000)
            with open(LOG_FILE,"a",newline="",encoding="utf-8") as f:
                csv.writer(f).writerow([now_str(),coin,f"{price:.4f}" if price else "",s15,s1h_pretty,s4h,f"{sc15:.2f}",f"{sc1h:.2f}",f"{sc4h:.2f}",funding,oi,vote,(note + (f" | {duration_ms}ms" if duration_ms else ""))])

            # === Log cảnh báo để tự học (dựa trên score & alert_dyn) ===
            if ind1h and price and ind1h.get("atr"):
                # hướng thiên theo score 1h (không phụ thuộc vote)
                dir_alert = "Long" if sc1h>0 else ("Short" if sc1h<0 else None)
                self._maybe_log_alert(coin, dir_alert, sc1h, alert_dyn, price, ind1h.get("atr"))

            # === Hai bước xác nhận (pending 15m) ===
            want_confirm = ("Xác nhận" in s1h)
            want_dir = ("Long" if "Long" in s1h else ("Short" if "Short" in s1h else None))
            now_ts = time.time()

            if want_confirm and st["pending_dir"] is None and not note.startswith("🧯"):
                st["pending_dir"] = want_dir
                st["pending_ready_at"] = now_ts + PENDING_CONFIRM_SEC
                st["last_sig_1h"]=s1h
                st["bars"]=0
                send_ok = False
                pending_reason = "⏳ pending confirm (2-step)"
            elif want_confirm and st["pending_dir"] == want_dir and now_ts >= st.get("pending_ready_at", 0):
                send_ok = True
                pending_reason = ""
                st["pending_dir"] = None
                st["pending_ready_at"] = 0.0
            else:
                send_ok = False
                pending_reason = ""
                if st["pending_dir"] and (not want_confirm or (want_dir and want_dir != st["pending_dir"])):
                    st["pending_dir"] = None
                    st["pending_ready_at"] = 0.0

            # Anti-spam 1h
            if send_ok:
                sig_key = f"{coin}|{s1h}|{int(datetime.now().timestamp()//3600)}"
                if self.last_signal_hash.get(coin) == sig_key:
                    send_ok = False
                else:
                    self.last_signal_hash[coin] = sig_key

            # Gửi & mở vị thế (ALERT_ONLY thì không mở)
            if send_ok and self.confirmed_sent<MAX_CONFIRMED_PER_ROUND and not self.tg.paused and price and not note.startswith("🧯"):
                direction="Long" if "Long" in s1h else "Short"
                lvls=propose_levels(price, ind1h.get("atr") if ind1h else None, direction)
                risk_pt=volatility_adjusted_risk(ind1h.get("atr") if ind1h else None, price) * self.tune.get(coin,{}).get("risk_factor",1.0)
                qty=round_qty(symbol, position_size(ACCOUNT_BALANCE, risk_pt, lvls["entry"], lvls["sl"]))
                if qty<=0:
                    send_ok=False
                entry=round_price(symbol, price*(1 + (SLIPPAGE_BPS/10000.0) * (1 if direction=="Long" else -1)))
                msg=[f"🤖 *BOT5 Ultra*", f"⏰ {now_str()}", f"📊 {coin}/USDT", f"🗳️ Vote: {vote}", f"1h: {s1h}"]
                if ALERT_ONLY:
                    msg.append(f"🔕 ALERT_ONLY: *không* mở vị thế")
                else:
                    if not PAPER_TRADE:
                        msg.append("⚠️ PAPER_TRADE=off chưa hỗ trợ lệnh thật — giữ nguyên paper.")
                    self.db.open_pos(coin, direction, qty, entry, lvls["sl"], lvls["tp1"], lvls["trail_step"], self.tg.run_id)
                    msg.append(f"🎯 {direction}: Entry~{entry:.4f} | TP1 {lvls['tp1']:.4f} | SL {lvls['sl']:.4f} | Trail±{lvls['trail_step']:.4f}")
                    msg.append(f"💼 Qty≈{qty:.6f} (risk~{risk_pt*100:.2f}%); Fees {FEE_RATE*100:.02f}%, Slippage {SLIPPAGE_BPS}bps")
                self.tg.send("\n".join(msg)); st["last_sig_1h"]=s1h; st["bars"]=0; self.confirmed_sent+=1
            else:
                st["bars"]+=1
                extra = f" | {pending_reason}" if pending_reason else ""
                note2 = (f" | {note}" if note else "")
                print(f"⏰ {now_str()} - {coin}: {s1h_pretty} | Vote: {vote}{note2}{extra}")
            st["last_bias_1h"]=new_bias

            # REST-only: cập nhật SL/TP/trailing
            if price: self.update_positions_with_price(coin, price)

            # Kích hoạt học nhẹ giữa vòng quét (không nặng)
            if LEARN_ENABLED: self.self_learn_if_needed(force=False)
            return price
        except Exception as e:
            self.error_alert(f"{coin} round error: {e}\n{traceback.format_exc()[:600]}")
            return None

    async def coin_task_rest(self, coin: str, index: int):
        # stagger khởi đầu: dãn đều các coin trong 1 chu kỳ
        initial_delay = (index % max(1, len(self.coins))) * (PACE_SEC / max(1, len(self.coins)))
        await asyncio.sleep(initial_delay)
        next_run = time.time()
        while True:
            try:
                if not self.tg.paused:
                    self.one_coin_round(coin)
                next_run += PACE_SEC
                sleep_for = max(0.0, next_run - time.time())
                await asyncio.sleep(sleep_for)
            except Exception as e:
                self.error_alert(f"coin_task_rest({coin}): {e}")
                await asyncio.sleep(1.0)

    async def price_watcher_task(self):
        while True:
            try:
                open_pos = self.db.get_open_positions()
                if open_pos:
                    for coin in list(open_pos.keys()):
                        price = binance_price(f"{coin}{QUOTE}")
                        if price:
                            self.update_positions_with_price(coin, price)
                await asyncio.sleep(max(1, PRICE_WATCH_SEC))
            except Exception as e:
                self.error_alert(f"price_watcher_task: {e}")
                await asyncio.sleep(2)

    async def telegram_task(self):
        while True:
            try:
                msgs=self.tg.poll()
                if msgs: self.handle_commands(msgs)
            except Exception as e: self.error_alert(f"tg poll: {e}")
            await asyncio.sleep(1.0)

    async def main(self):
        self.tg.send(f"🚀 *BOT5 Ultra* (REST-only, Self-Learning v3) đang chạy — {now_str()}")
        tasks=[]; tg_task=asyncio.create_task(self.telegram_task())
        price_task=asyncio.create_task(self.price_watcher_task())
        while True:
            try:
                self.heartbeat_if_needed(); self.refresh_watchlist(); self.housekeeping_if_needed()
                if (not tasks) or (len(tasks)!=len(self.coins)):
                    for t in tasks: t.cancel()
                    tasks=[asyncio.create_task(self.coin_task_rest(c, i)) for i,c in enumerate(self.coins)]
                write_status({"time":now_str(),"state":"running","coins":self.coins,"paused":self.tg.paused,"pace_sec": PACE_SEC,
                              "alert_only": ALERT_ONLY, "paper_trade": PAPER_TRADE, "learn_enabled": LEARN_ENABLED})
                self.confirmed_sent=0
                await asyncio.sleep(SLEEP_BETWEEN_ROUNDS)
                print("🔄 Bot 8 REST-only (Self-Learning v3) đang chạy... (xong 1 vòng)")
            except Exception as e:
                self.error_alert(f"main loop: {e}")
                write_status({"time":now_str(),"state":"error","last_error":str(e)})
                await asyncio.sleep(3)

if __name__=="__main__":
    bot=Bot5Ultra()
    try: asyncio.run(bot.main())
    except KeyboardInterrupt: print("Bye")
