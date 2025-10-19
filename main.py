import ccxt
import talib
import numpy as np
import logging
import asyncio
from telegram import Bot

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = "8118417455:AAFyUEHeh-JzyUL9s51Ab7r69LuvVdcd364"
CHAT_ID = "-4804203693"
SYMBOL = "BTC/USDT"
EMA_PERIOD = 21
TF_LIST = ['1h', '4h', '1d']
RISK_PERCENT = 1    # % vốn mỗi lệnh
BALANCE = 1000      # USD tài khoản (cập nhật đúng số dư của bạn)
RISK_REWARD_RATIO = 2
ATR_MULT = 1.5
SWING_LOOKBACK = 5
FETCH_LIMIT = 50

bot = Bot(token=TELEGRAM_TOKEN)
exchange = ccxt.okx()

def fetch_ohlcv(symbol, timeframe, limit=FETCH_LIMIT):
    arr = np.array(exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit))
    return arr[:, 1].astype(float), arr[:, 2].astype(float), arr[:, 3].astype(float), arr[:, 4].astype(float), arr[:, 5].astype(float)

def detect_trend(close):
    ema = talib.EMA(close, EMA_PERIOD)
    if close[-1] > ema[-1] and close[-1] > close[-2]:
        return 'Up', ema[-1]
    elif close[-1] < ema[-1] and close[-1] < close[-2]:
        return 'Down', ema[-1]
    return 'Sideway', ema[-1]

def detect_patterns(open_, high, low, close):
    patterns = []
    if talib.CDLENGULFING(open_, high, low, close)[-1] > 0:
        patterns.append('Bullish Engulfing')
    if talib.CDLENGULFING(open_, high, low, close)[-1] < 0:
        patterns.append('Bearish Engulfing')
    if talib.CDLHAMMER(open_, high, low, close)[-1] != 0:
        patterns.append('Hammer')
    if talib.CDLSHOOTINGSTAR(open_, high, low, close)[-1] != 0:
        patterns.append('Shooting Star')
    if talib.CDLDOJI(open_, high, low, close)[-1] != 0:
        patterns.append('Doji')
    return patterns

def get_rsi(close):
    return float(talib.RSI(close, 14)[-1])

def get_macd(close):
    macd, signal, hist = talib.MACD(close)
    return float(macd[-1]), float(signal[-1]), float(hist[-1])

def get_swing_low(low, lookback=SWING_LOOKBACK):
    # Đáy thấp nhất trong N nến gần đây, ngoại trừ nến tín hiệu
    return float(np.min(low[-lookback-1:-1]))

def get_swing_high(high, lookback=SWING_LOOKBACK):
    # Đỉnh cao nhất trong N nến gần đây, ngoại trừ nến tín hiệu
    return float(np.max(high[-lookback-1:-1]))

def calculate_trade(open_, high, low, close, decision, balance, risk_percent, rr_ratio, atr_mult, swing_lookback):
    entry_price = float(close[-1])
    atr = float(talib.ATR(high, low, close, timeperiod=14)[-1])
    # Tính SL kết hợp ATR và swing, đảm bảo không nhầm logic
    if decision == "Long":
        sl_atr = entry_price - atr * atr_mult
        sl_swing = get_swing_low(low, lookback=swing_lookback)
        sl = max(sl_atr, sl_swing)  # Đặt SL an toàn hơn
        risk = entry_price - sl
        tp = entry_price + rr_ratio * risk
    elif decision == "Short":
        sl_atr = entry_price + atr * atr_mult
        sl_swing = get_swing_high(high, lookback=swing_lookback)
        sl = min(sl_atr, sl_swing)
        risk = sl - entry_price
        tp = entry_price - rr_ratio * risk
    else:
        sl = tp = None
        risk = 0

    if risk > 0:
        position_size = (balance * (risk_percent / 100)) / risk
    else:
        position_size = 0

    # In log kiểm tra số liệu thực chiến
    logging.info(f"[DEBUG] Entry: {entry_price:.2f} | SL: {sl:.2f} | TP: {tp:.2f} | ATR: {atr:.2f} | Decision: {decision}")
    return round(entry_price, 2), round(sl, 2), round(tp, 2), round(position_size, 4)

def multi_timeframe_analyze():
    result = {}
    for tf in TF_LIST:
        open_, high, low, close, _ = fetch_ohlcv(SYMBOL, tf)
        trend, ema = detect_trend(close)
        patterns = detect_patterns(open_, high, low, close)
        result[tf] = {
            'trend': trend,
            'ema': round(float(ema), 2),
            'close': round(float(close[-1]), 2),
            'patterns': patterns,
            'rsi': round(get_rsi(close), 2),
            'macd_hist': round(get_macd(close)[2], 2)
        }
    return result

def decision_logic(analysis):
    trends = [analysis[tf]['trend'] for tf in TF_LIST]
    if all(tr == "Up" for tr in trends):
        return "Long"
    if all(tr == "Down" for tr in trends):
        return "Short"
    return "No trade"

async def send_telegram(msg):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(f"Telegram error: {e}")

async def main():
    last_msg = ""
    while True:
        try:
            analysis = multi_timeframe_analyze()
            decision = decision_logic(analysis)

            if decision in ["Long", "Short"]:
                msg = f"📢 🚨 Tín hiệu giao dịch BTC/USDT (OKX) 🚨 📢\n"
                for tf, tr in analysis.items():
                    pat = ", ".join(tr['patterns']) if tr['patterns'] else "Không có"
                    msg += f"[{tf}] {tr['trend']} | Giá: {tr['close']:.2f} | EMA: {tr['ema']:.2f}\n"
                    msg += f"  Nến: {pat}\n"
                    msg += f"  RSI: {tr['rsi']} | MACD Hist: {tr['macd_hist']}\n"
                open_, high, low, close, _ = fetch_ohlcv(SYMBOL, '1h')
                entry, sl, tp, size = calculate_trade(
                    open_, high, low, close,
                    decision,
                    BALANCE, RISK_PERCENT, RISK_REWARD_RATIO, ATR_MULT, SWING_LOOKBACK
                )
                msg += f"\n✅ Tín hiệu: {decision}\nEntry: {entry:.2f} | SL: {sl:.2f} | TP: {tp:.2f}\nKhối lượng: {size} BTC"

                if msg != last_msg:
                    await send_telegram(msg)
                    logging.info(msg)
                    last_msg = msg
            else:
                logging.info("Đang theo dõi thị trường, chưa có tín hiệu mới...")

            await asyncio.sleep(180)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())