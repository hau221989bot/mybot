#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XSMB Soi 1 Số — Thống kê thông minh (dễ dùng nhất)
Python 3.9+

CÁCH DÙNG CỰC DỄ (không tham số):
    py "lô.py"
→ Tự:
   1) Đọc xs_mb.csv (cùng thư mục)
   2) Nếu có config.json thì dùng tham số trong đó; nếu chưa, auto-tune nhanh
   3) Dự đoán 1 số cho ngày cuối trong CSV, in Confidence

LỆNH BỔ SUNG:
    py "lô.py" --update                 # Lấy XSMB hôm nay (sau 18:15) và thêm vào xs_mb.csv
    py "lô.py" --update-history 365     # Đổ lịch sử lùi 365 ngày (bỏ qua ngày đã có)
    py "lô.py" --learn                  # Auto-tune tham số & lưu config.json
    py "lô.py" --backtest               # Đánh giá độ trúng (walk-forward)

Lưu ý: Công cụ thống kê phục vụ theo dõi/cảnh báo. Không đảm bảo kết quả.
"""

import argparse, csv, json, math, os, re, sys, time
from datetime import datetime, timedelta
from collections import Counter
from typing import List, Tuple, Dict

# ===== Đường dẫn mặc định =====
DATA_FILE = os.path.join(os.path.dirname(__file__), "xs_mb.csv")
CONF_FILE = os.path.join(os.path.dirname(__file__), "config.json")

# ===== IO / CSV =====
def parse_row(row) -> Tuple[datetime.date, List[str]]:
    date_str = row[0].strip()
    nums_str = row[1].strip() if len(row) > 1 else ""
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    nums = []
    for t in re.findall(r"\d{1,2}", nums_str):
        n = int(t) % 100
        nums.append(f"{n:02d}")
    return d, nums

def load_csv(path: str) -> List[Tuple[datetime.date, List[str]]]:
    if not os.path.isfile(path):
        return []
    data = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or not str(row[0]).strip() or str(row[0]).startswith("#"):
                continue
            d, nums = parse_row(row)
            data.append((d, nums))
    data.sort(key=lambda x: x[0])
    return data

def save_row(path: str, d: datetime.date, nums: List[str]):
    line = f"{d.isoformat()},{' '.join(nums)}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)

# ===== Utils =====
def slice_window(data, end_date, days):
    start = end_date - timedelta(days=days)
    return [(d, nums) for (d, nums) in data if start <= d <= end_date]

def last_seen_map(data):
    last = {}
    for d, nums in data:
        for n in nums:
            last[n] = d
    return last

def freq_in_window(window_records) -> Counter:
    c = Counter()
    for _, nums in window_records:
        c.update(nums)
    return c

def normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
    if not scores:
        return {}
    vals = list(scores.values())
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return {k: 0.5 for k in scores}
    return {k: (v - lo) / (hi - lo) for k, v in scores.items()}

# ===== Signals =====
def ewma_freq(window_records, decay_lambda: float = 0.9) -> Dict[str, float]:
    if not window_records:
        return {}
    window_records = sorted(window_records, key=lambda x: x[0])
    last_day = window_records[-1][0]
    scores = Counter()
    for d, nums in window_records:
        age = (last_day - d).days
        w = (decay_lambda ** age)
        for n in nums:
            scores[n] += w
    return dict(scores)

def overdue_adjusted(window_records, all_data, end_date) -> Dict[str, float]:
    freq = freq_in_window(window_records)
    last_map = last_seen_map(all_data)
    out = {}
    for n, f in freq.items():
        last = last_map.get(n, None)
        gap = (end_date - last).days if last else 9999
        out[n] = f * math.log(1 + gap)
    return out

def markov_from_yesterday(all_data, end_date, lookback_days=90, use_two_days=True) -> Dict[str, float]:
    hist = slice_window(all_data, end_date, lookback_days)
    hist = sorted(hist, key=lambda x: x[0])
    if len(hist) < 2:
        return {}
    trans = Counter()
    next_counts = Counter()
    by_date = {d: nums for d, nums in hist}
    days = sorted(by_date.keys())
    for i in range(len(days) - 1):
        d_cur, d_nxt = days[i], days[i + 1]
        set_cur = set(by_date[d_cur])
        set_nxt = set(by_date[d_nxt])
        for y in set_cur:
            next_counts[y] += 1
            for c in set_nxt:
                trans[(y, c)] += 1
    def p_c_given_y(c, y):
        denom = next_counts.get(y, 0)
        if denom == 0:
            return 0.0
        return trans.get((y, c), 0) / float(denom)
    yester = max([d for d in days if d <= end_date], default=None)
    if yester is None:
        return {}
    prior_nums = set(by_date.get(yester, []))
    if use_two_days:
        idx = days.index(yester)
        if idx - 1 >= 0:
            prior_nums |= set(by_date.get(days[idx - 1], []))
    all_candidates = set()
    for _, nums in hist:
        all_candidates.update(nums)
    if not prior_nums:
        return {n: 0.0 for n in all_candidates}
    scores = {}
    for n in all_candidates:
        s = 0.0
        for y in prior_nums:
            s += p_c_given_y(n, y)
        scores[n] = s / len(prior_nums)
    return scores

def weekday_boost_scores(all_data, end_date, weeks=8) -> Dict[str, float]:
    target_wd = end_date.weekday()
    hist = [(d, nums) for d, nums in all_data if d.weekday() == target_wd and d < end_date]
    hist = sorted(hist, key=lambda x: x[0])[-weeks:]
    c = Counter()
    for _, nums in hist:
        c.update(set(nums))  # đếm theo NGÀY (unique)
    return dict(c)

def recent_penalty_scores(all_data, end_date, days=2, penalty=0.8) -> Dict[str, float]:
    """Phạt nặng các số vừa về trong N ngày gần nhất (default: 2 ngày)."""
    recent = set()
    for k in range(1, days + 1):
        day = end_date - timedelta(days=k)
        for d, nums in all_data:
            if d == day:
                recent.update(nums)
    return {n: -penalty for n in recent}

# ===== Ensemble & Pick =====
def ensemble_pick(window_records, all_data, end_date,
                  w1=0.45, w2=0.35, w3=0.20,
                  decay_lambda=0.9,
                  markov_lookback=90,
                  use_two_days=True) -> Tuple[str, Dict]:
    sig1 = ewma_freq(window_records, decay_lambda=decay_lambda)
    sig2 = overdue_adjusted(window_records, all_data, end_date)
    sig3 = markov_from_yesterday(all_data, end_date, lookback_days=markov_lookback, use_two_days=use_two_days)
    sig4 = weekday_boost_scores(all_data, end_date, weeks=8)
    sig5 = recent_penalty_scores(all_data, end_date, days=2, penalty=0.8)

    # Pool: mọi số từng xuất hiện trong cửa sổ (fallback: toàn bộ lịch sử)
    pool = set()
    for d, nums in window_records:
        pool.update(nums)
    if not pool:
        for _, nums in all_data:
            pool.update(nums)
    if not pool:
        return None, {"reason": "no_candidates"}

    n1 = normalize_scores({k: sig1.get(k, 0.0) for k in pool})
    n2 = normalize_scores({k: sig2.get(k, 0.0) for k in pool})
    n3 = normalize_scores({k: sig3.get(k, 0.0) for k in pool})
    n4 = normalize_scores({k: sig4.get(k, 0.0) for k in pool})

    final = {}
    for k in pool:
        base = (w1 * n1.get(k, 0.5) +
                w2 * n2.get(k, 0.5) +
                w3 * n3.get(k, 0.5))
        boost = 0.15 * n4.get(k, 0.0)
        penalty = sig5.get(k, 0.0)
        final[k] = base + boost + penalty

    # Tie-break: điểm cao nhất -> gap lớn hơn -> số nhỏ hơn
    last_map = last_seen_map(all_data)
    def gap_days(n):
        last = last_map.get(n, None)
        return (end_date - last).days if last else 9999

    best_score = max(final.values())
    cands = [k for k, v in final.items() if v == best_score]
    best_gap = max(gap_days(n) for n in cands)
    cands2 = [n for n in cands if gap_days(n) == best_gap]
    pick = min(cands2)

    # Confidence
    scores_sorted = sorted(final.values())
    top = scores_sorted[-1]
    second = scores_sorted[-2] if len(scores_sorted) >= 2 else scores_sorted[-1]
    denom = (max(final.values()) - min(final.values())) or 1.0
    confidence = max(0.0, min(1.0, (top - second) / denom))

    explain = {
        "weights": {"w1": w1, "w2": w2, "w3": w3, "weekday_boost": 0.15, "recent_penalty": 0.8},
        "decay_lambda": decay_lambda,
        "markov_lookback": markov_lookback,
        "use_two_days": use_two_days,
        "top_score": round(best_score, 6),
        "tie_candidates": sorted(cands),
        "tie_after_gap": sorted(cands2),
        "final_gap_days": gap_days(pick),
        "confidence": round(confidence, 3),
    }
    return pick, explain

# ===== Backtest & Auto-tune =====
def day_hit(nums_of_day: List[str], pick: str) -> bool:
    return pick in set(nums_of_day)

def walk_forward_backtest(data, win=45, train=60,
                          w1=0.45, w2=0.35, w3=0.20,
                          decay=0.9, markov_lb=90, two_days=True):
    days = [d for d, _ in data]
    hits, total = 0, 0
    max_dd, cur_dd = 0, 0
    for i in range(train + 1, len(days)):
        end_date = days[i - 1]
        wrec = slice_window(data, end_date, win)
        if not wrec:
            continue
        pick, _ = ensemble_pick(
            wrec, [rec for rec in data if rec[0] <= end_date], end_date,
            w1=w1, w2=w2, w3=w3, decay_lambda=decay, markov_lookback=markov_lb, use_two_days=two_days
        )
        if not pick:
            continue
        test_date, nums = data[i]
        hit = day_hit(nums, pick)
        hits += int(hit); total += 1
        if hit:
            max_dd = max(max_dd, cur_dd); cur_dd = 0
        else:
            cur_dd += 1
    max_dd = max(max_dd, cur_dd)
    rate = (hits / total) if total else 0.0
    return {"hits": hits, "total": total, "hit_rate": rate, "max_drawdown_days": max_dd}

def autotune_params(data, train=60):
    WINS       = [30, 45, 60]
    DECAYS     = [0.85, 0.9, 0.95]
    MARKOV_LB  = [60, 90, 120]
    TWO_DAYS   = [True, False]
    WEIGHTS    = [(0.5,0.3,0.2),(0.45,0.35,0.2),(0.4,0.4,0.2),(0.33,0.33,0.34)]
    best = None
    for win in WINS:
        for decay in DECAYS:
            for lb in MARKOV_LB:
                for two in TWO_DAYS:
                    for (w1,w2,w3) in WEIGHTS:
                        res = walk_forward_backtest(
                            data, win=win, train=train,
                            w1=w1, w2=w2, w3=w3,
                            decay=decay, markov_lb=lb, two_days=two
                        )
                        score = res["hit_rate"] - 0.0005*res["max_drawdown_days"]
                        params = {"win":win,"decay":decay,"markov_lb":lb,"two_days":two,"w1":w1,"w2":w2,"w3":w3}
                        if best is None or score > best[0]:
                            best = (score, params, res)
    return best  # (score, params, res)

# ===== Lấy dữ liệu web: hôm nay & lịch sử =====
def _fetch_xsmb_by_url(url: str) -> list:
    """Tải 1 URL và cố gắng trích 27 số 2 chữ số cuối (trả về [] nếu thất bại)."""
    import requests
    try:
        r = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        html = r.text
        nums = re.findall(r"\d{2,6}", html)
        last2 = [f"{int(x)%100:02d}" for x in nums]
        cand = last2[-200:]           # lấy đuôi rộng
        if len(cand) >= 27:
            return cand[-27:]
    except Exception:
        pass
    return []

def fetch_xsmb_today() -> Tuple[datetime.date, List[str]]:
    today = datetime.now().date()
    for url in [
        "https://xskt.com.vn/xsmb",
        "https://ketqua.me/xo-so-mien-bac",
        "https://www.minhngoc.net.vn/rss.html"  # fallback thô (nếu cần)
    ]:
        out = _fetch_xsmb_by_url(url)
        if len(out) >= 27:
            return today, out[-27:]
    raise RuntimeError("Không lấy được dữ liệu XSMB hôm nay (nguồn web thay đổi hoặc chưa có kết quả).")

def fetch_xsmb_by_date(d: datetime.date) -> list:
    ddmmyyyy = d.strftime("%d-%m-%Y")
    dd_mm_yyyy_slash = d.strftime("%d/%m/%Y")
    candidates = [
        f"https://ketqua.me/xo-so-mien-bac-ngay-{ddmmyyyy}",
        f"https://www.minhngoc.net.vn/xo-so-mien-bac/{ddmmyyyy}.html",
        f"https://xskt.com.vn/xsmb?ngay={dd_mm_yyyy_slash}",
        "https://ketqua.me/xo-so-mien-bac",
        "https://xskt.com.vn/xsmb",
    ]
    for url in candidates:
        out = _fetch_xsmb_by_url(url)
        if len(out) >= 27:
            return out[-27:]
    return []

# ===== Config save/load =====
def load_config():
    if os.path.isfile(CONF_FILE):
        try:
            with open(CONF_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_config(params: Dict):
    with open(CONF_FILE, "w", encoding="utf-8") as f:
        json.dump(params, f, ensure_ascii=False, indent=2)

# ===== Actions =====
def action_predict():
    data = load_csv(DATA_FILE)
    if not data:
        print(f"❌ Chưa có dữ liệu: {DATA_FILE}")
        print('→ Sau 18:15 chạy:  py "lô.py" --update  hoặc đổ lịch sử:  py "lô.py" --update-history 365')
        sys.exit(1)

    conf = load_config()
    if conf is None:
        print("🔧 Chưa có config → auto-tune nhanh trên dữ liệu gần đây…")
        best = autotune_params(data, train=min(60, max(10, len(data)//3)))
        if best is None:
            print("❌ Không đủ dữ liệu để tự học.")
            sys.exit(1)
        _, params, res = best
        save_config(params)
        conf = params
        print(f"✅ Lưu config.json: {params} | Backtest hit: {res['hit_rate']:.2%}")

    end_date = max(d for d,_ in data)
    wrec = slice_window(data, end_date, conf.get("win",45))
    pick, info = ensemble_pick(
        wrec, data, end_date,
        w1=conf.get("w1",0.45), w2=conf.get("w2",0.35), w3=conf.get("w3",0.20),
        decay_lambda=conf.get("decay",0.9),
        markov_lookback=conf.get("markov_lb",90),
        use_two_days=conf.get("two_days", True)
    )
    if not pick:
        print("❌ Không chọn được số (thiếu dữ liệu)."); sys.exit(1)

    print("📌 XSMB Soi 1 Số — Thống kê thông minh")
    print(f"Ngày tham chiếu: {end_date} | Cửa sổ: {conf.get('win',45)} ngày")
    print(f"Tham số: {conf}")
    print(f"✅ Số đề cử: {pick}")
    print(f"• Confidence: {info.get('confidence')}")
    print(f"• Tie-candidates: {', '.join(info.get('tie_candidates', []))}")
    print(f"• Tie-after-gap : {', '.join(info.get('tie_after_gap', []))}")
    print(f"• Final gap days: {info.get('final_gap_days')}")

def action_learn():
    data = load_csv(DATA_FILE)
    if not data:
        print(f"❌ Chưa có dữ liệu: {DATA_FILE}")
        sys.exit(1)
    print("🔧 Đang auto-tune tham số (tự học)…")
    best = autotune_params(data, train=min(60, max(10, len(data)//3)))
    if best is None:
        print("❌ Không đủ dữ liệu để tự học.")
        sys.exit(1)
    _, params, res = best
    save_config(params)
    print("✅ Đã lưu config.json:", params)
    print(f"📊 Backtest: {res['hits']}/{res['total']} ngày | HitRate = {res['hit_rate']:.2%} | MaxDD = {res['max_drawdown_days']}d")

def action_backtest():
    data = load_csv(DATA_FILE)
    if not data:
        print(f"❌ Chưa có dữ liệu: {DATA_FILE}")
        sys.exit(1)
    conf = load_config() or {"win":45,"w1":0.45,"w2":0.35,"w3":0.20,"decay":0.9,"markov_lb":90,"two_days":True}
    res = walk_forward_backtest(
        data, win=conf["win"], train=min(60, max(10, len(data)//3)),
        w1=conf["w1"], w2=conf["w2"], w3=conf["w3"],
        decay=conf["decay"], markov_lb=conf["markov_lb"], two_days=conf.get("two_days", True)
    )
    print(f"📊 Backtest: {res['hits']}/{res['total']} ngày | HitRate = {res['hit_rate']:.2%} | MaxDD = {res['max_drawdown_days']}d")

def action_update():
    print("🌐 Đang lấy kết quả XSMB hôm nay…")
    try:
        d, nums27 = fetch_xsmb_today()
    except Exception as e:
        print("❌ Không lấy được dữ liệu hôm nay:", e)
        sys.exit(1)

    if len(nums27) < 27:
        print("❌ Dữ liệu không đủ 27 số:", nums27)
        sys.exit(1)

    data = load_csv(DATA_FILE)
    if any(d == x[0] for x in data):
        print(f"ℹ️ Ngày {d} đã có trong {DATA_FILE} → bỏ qua.")
        return

    save_row(DATA_FILE, d, nums27)
    print(f"✅ Đã thêm {d} vào {DATA_FILE}:")
    print("   ", " ".join(nums27))
    print('👉 Gợi ý: chạy tiếp  py "lô.py" --learn  rồi  py "lô.py"')

def action_update_history(days_back: int):
    data_existing = load_csv(DATA_FILE)
    have = {d for d,_ in data_existing}
    today = datetime.now().date()
    start = today - timedelta(days=days_back)
    added = 0; skipped = 0; failed = 0

    print(f"⏬ Đang lấy lịch sử XSMB từ {start} → {today - timedelta(days=1)} ...")
    cur = start
    while cur < today:
        if cur in have:
            skipped += 1
            cur += timedelta(days=1)
            continue
        nums27 = fetch_xsmb_by_date(cur)
        if len(nums27) >= 27:
            save_row(DATA_FILE, cur, nums27[-27:])
            print(f"  ✅ {cur}: {' '.join(nums27[-27:])}")
            added += 1
        else:
            print(f"  ❌ {cur}: không lấy được (nguồn thay đổi hoặc không có dữ liệu).")
            failed += 1
        time.sleep(0.8)  # lịch sự với server
        cur += timedelta(days=1)

    print(f"Hoàn tất: +{added} ngày, bỏ qua {skipped} (đã có), thất bại {failed}.")
    if added == 0:
        print("ℹ️ Không có ngày mới nào được thêm. Có thể CSV của bạn đã đủ dày.")
    else:
        print('👉 Nên chạy tiếp:  py "lô.py" --learn  rồi  py "lô.py"')

# ===== Main =====
def main():
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--update", action="store_true", help="Lấy XSMB hôm nay và append CSV")
    ap.add_argument("--update-history", type=int, help="Lấy lùi N ngày lịch sử (vd 365)")
    ap.add_argument("--learn", action="store_true", help="Auto-tune tham số và lưu config.json")
    ap.add_argument("--backtest", action="store_true", help="Đánh giá độ trúng (walk-forward)")
    args, _ = ap.parse_known_args()

    if args.update:
        action_update()
    elif args.update_history is not None:
        n = max(1, int(args.update_history))
        action_update_history(n)
    elif args.learn:
        action_learn()
    elif args.backtest:
        action_backtest()
    else:
        # mặc định: soi 1 số (dễ nhất)
        if not os.path.isfile(DATA_FILE):
            print(f"❌ Không thấy {DATA_FILE}.")
            print('→ Sau 18:15 chạy:  py "lô.py" --update  hoặc:  py "lô.py" --update-history 365')
            return
        action_predict()

if __name__ == "__main__":
    # Gõ "py lô.py" là chạy luôn (predict)
    main()
