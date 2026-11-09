"""Бенчмарк Q1–Q5: SQL view, materialized view і MongoDB."""

import argparse
import csv
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
# Спільний конфіг лежить у корені проєкту.
sys.path.insert(0, str(ROOT))
from config import MONGO_URL, PG_URL

PROBE_PID = 1
WRITE_BASE = 9_000_000_000

SQL_Q1 = "SELECT card FROM lab2.v_project_card WHERE p_uid = %s"
SQL_Q1M = "SELECT card FROM lab2.mv_project_card WHERE p_uid = %s"
SQL_Q2 = """
SELECT p_uid, title, budget_max, company_name, proposals_count, category_slug
  FROM lab2.v_project_card
 WHERE status = 'OPEN' AND category_slug = %s AND budget_max >= %s
 ORDER BY created_at DESC LIMIT 20
"""
SQL_Q3 = """
SELECT p_uid, title FROM lab2.project
 WHERE status = 'OPEN' AND is_deleted = false AND %s = ANY(skills)
 ORDER BY created_at DESC LIMIT 20
"""
SQL_Q4 = """
SELECT cat_uid, count(*) AS n, round(avg(budget_max), 2) AS avg_budget
  FROM lab2.project WHERE status = 'OPEN' AND is_deleted = false
 GROUP BY cat_uid ORDER BY cat_uid
"""
SQL_Q5 = "INSERT INTO lab2.proposal (pr_uid, p_uid, bid_amount, created_at) VALUES (%s,%s,%s,%s)"

SQL_Q2M = """
SELECT p_uid, title, budget_max, company_name, proposals_count, category_slug
  FROM lab2.mv_project_card
 WHERE status = 'OPEN' AND category_slug = %s AND budget_max >= %s
 ORDER BY created_at DESC LIMIT 20
"""
SQL_Q3M = """
SELECT p_uid, title FROM lab2.mv_project_card
 WHERE status = 'OPEN' AND %s = ANY(skills)
 ORDER BY created_at DESC LIMIT 20
"""
SQL_Q4M = """
SELECT cat_uid, count(*) AS n, round(avg(budget_max), 2) AS avg_budget
  FROM lab2.mv_project_card WHERE status = 'OPEN'
 GROUP BY cat_uid ORDER BY cat_uid
"""

SLUG, MIN_BUDGET, SKILL = "dev", 1000.0, "python"


def _utc(value):
    """Час до UTC ISO: PostgreSQL і BSON віддають дати в різних зонах."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def canon(value):
    """Канонізує результат, щоб порівнювати SQL і MongoDB."""
    if isinstance(value, Decimal):
        return round(float(value), 2)
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return value
    if isinstance(value, dict):
        return {k: canon(v) for k, v in sorted(value.items()) if v is not None}
    if isinstance(value, (list, tuple)):
        return [canon(v) for v in value]
    return value


class Ctx:
    """Одне з'єднання на потік вимірювання."""

    def __init__(self, store):
        """Відкриває PostgreSQL або MongoDB залежно від варіанта."""
        self.pg = psycopg.connect(PG_URL) if store.startswith("sql") else None
        self.db = MongoClient(MONGO_URL)["freelancer_lab2"] if store == "mongodb" else None

    def close(self):
        """Закриває з'єднання PostgreSQL."""
        if self.pg:
            self.pg.close()


def _rows(cur):
    """Рядки курсора у список словників."""
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def sql_q1(ctx, i, pid):
    """Q1: картка за id з view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q1, (pid,))
        row = c.fetchone()
        return row[0] if row else None


def sql_q1m(ctx, i, pid):
    """Q1: картка за id з materialized view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q1M, (pid,))
        row = c.fetchone()
        return row[0] if row else None


def sql_q2(ctx, i, pid):
    """Q2: стрічка відкритих замовлень із view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q2, (SLUG, MIN_BUDGET))
        return [{"id": r["p_uid"], "title": r["title"], "budget": r["budget_max"],
                 "company": r["company_name"], "proposals": r["proposals_count"]}
                for r in _rows(c)]


def sql_q3(ctx, i, pid):
    """Q3: пошук за навичкою через ANY(skills)."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q3, (SKILL,))
        return [{"id": r["p_uid"], "title": r["title"]} for r in _rows(c)]


def sql_q4(ctx, i, pid):
    """Q4: агрегат бюджетів по категоріях."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q4)
        return [{"id": r["cat_uid"], "n": r["n"], "avg": r["avg_budget"]}
                for r in _rows(c)]


def sql_q5(ctx, i, pid):
    """Q5: запис відгуку в SQL."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q5, (WRITE_BASE + i, PROBE_PID, Decimal("1200.00"),
                           datetime.now(timezone.utc)))
    ctx.pg.commit()


def sql_q2m(ctx, i, pid):
    """Контроль Q2 на materialized view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q2M, (SLUG, MIN_BUDGET))
        return [{"id": r["p_uid"], "title": r["title"], "budget": r["budget_max"],
                 "company": r["company_name"], "proposals": r["proposals_count"]}
                for r in _rows(c)]


def sql_q3m(ctx, i, pid):
    """Контроль Q3 на materialized view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q3M, (SKILL,))
        return [{"id": r["p_uid"], "title": r["title"]} for r in _rows(c)]


def sql_q4m(ctx, i, pid):
    """Контроль Q4 на materialized view."""
    with ctx.pg.cursor() as c:
        c.execute(SQL_Q4M)
        return [{"id": r["cat_uid"], "n": r["n"], "avg": r["avg_budget"]}
                for r in _rows(c)]


def mongo_q1(ctx, i, pid):
    """Q1: документ картки за _id."""
    return ctx.db.project_cards.find_one({"_id": pid})


def mongo_q2(ctx, i, pid):
    """Q2: стрічка по compound-індексу ix_feed."""
    docs = ctx.db.project_cards.find(
        {"status": "OPEN", "category.slug": SLUG, "pricing.max": {"$gte": MIN_BUDGET}}
    ).sort("createdAt", -1).limit(20)
    return [{"id": d["_id"], "title": d["title"], "budget": d["pricing"]["max"],
             "company": d["client"].get("company"), "proposals": d["stats"]["proposals"]}
            for d in docs]


def mongo_q3(ctx, i, pid):
    """Q3: пошук за навичкою по масиву skills."""
    docs = ctx.db.project_cards.find(
        {"status": "OPEN", "skills": SKILL}
    ).sort("createdAt", -1).limit(20)
    return [{"id": d["_id"], "title": d["title"]} for d in docs]


def mongo_q4(ctx, i, pid):
    """Q4: агрегат бюджетів через $group."""
    docs = ctx.db.project_cards.aggregate([
        {"$match": {"status": "OPEN"}},
        {"$group": {"_id": "$category.id", "n": {"$sum": 1},
                    "avg": {"$avg": "$pricing.max"}}},
        {"$sort": {"_id": 1}},
    ])
    return [{"id": d["_id"], "n": d["n"], "avg": d["avg"]} for d in docs]


def mongo_q5(ctx, i, pid):
    """Q5: запис відгуку плюс $inc лічильника в картці."""
    ctx.db.proposals.insert_one({
        "_id": WRITE_BASE + i, "p_uid": PROBE_PID, "bidAmount": 1200.0,
        "createdAt": datetime.now(timezone.utc),
    })
    ctx.db.project_cards.update_one({"_id": PROBE_PID}, {"$inc": {"stats.proposals": 1}})


# Контроль mv відділяє ефект денормалізації від самого рушія БД.
CASES = [
    ("Q1 картка за id", sql_q1, mongo_q1, sql_q1m),
    ("Q2 стрічка замовлень", sql_q2, mongo_q2, sql_q2m),
    ("Q3 пошук за навичкою", sql_q3, mongo_q3, sql_q3m),
    ("Q4 агрегат по категоріях", sql_q4, mongo_q4, sql_q4m),
    ("Q5 додати відгук (запис)", sql_q5, mongo_q5, None),
]


def _same(left, right, is_aggregate):
    """Звіряє набори; для Q4 допускає похибку avg до 0.02."""
    if not is_aggregate:
        return left == right, 0.0
    keys = lambda rows: [{k: v for k, v in a.items() if k != "avg"} for a in rows]
    drift = max((abs(a["avg"] - b["avg"]) for a, b in zip(left, right)), default=0)
    return keys(left) == keys(right) and drift <= 0.02, drift


def verify(pid):
    """Не пускає бенчмарк, поки view, mv і MongoDB не збігаються."""
    sql, mdb = Ctx("sql"), Ctx("mongodb")
    ok = True
    for name, sfn, mfn, mvfn in CASES[:4]:
        aggregate = name.startswith("Q4")
        left = canon(sfn(sql, 0, pid))
        right = canon(mfn(mdb, 0, pid))
        same, drift = _same(left, right, aggregate)
        if not same:
            ok = False
            print(f"  РОЗБІЖНІСТЬ {name}: SQL={str(left)[:120]} | Mongo={str(right)[:120]}")
        else:
            tail = f" (похибка середнього {drift:.4f})" if aggregate else ""
            print(f"  {name}: набори результатів однакові{tail}")
        if mvfn:
            control = canon(mvfn(sql, 0, pid))
            same_mv, _ = _same(left, control, aggregate)
            if not same_mv:
                ok = False
                print(f"  РОЗБІЖНІСТЬ {name}: materialized view ≠ view")
            else:
                print(f"  {name}: контроль mv_project_card збігається")
    sql.close()
    mdb.close()
    return ok


def measure(store, fn, pid, reps, concurrency):
    """Прогрів + reps замірів, послідовно або в пулі потоків."""
    warm = 5

    def worker(indices):
        """Один потік: своє з'єднання й заміри по своїх індексах."""
        ctx = Ctx(store)
        local = []
        for i in indices:
            t0 = time.perf_counter()
            fn(ctx, i, pid)
            local.append((time.perf_counter() - t0) * 1000)
        ctx.close()
        return local

    # Прогрів не потрапляє у статистику.
    worker(range(warm))
    if concurrency == 1:
        lat = worker(range(warm, warm + reps))
    else:
        chunks = [[] for _ in range(concurrency)]
        for j, i in enumerate(range(warm, warm + reps)):
            chunks[j % concurrency].append(i)
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            lat = [x for part in pool.map(worker, chunks) for x in part]
    return lat


def percentile(data, p):
    """Перцентиль без numpy."""
    s = sorted(data)
    return s[max(0, math.ceil(p / 100 * len(s)) - 1)]


def plans(pid):
    """Плани Q2: EXPLAIN ANALYZE й executionStats."""
    ctx = Ctx("sql")
    with ctx.pg.cursor() as c:
        c.execute("EXPLAIN (ANALYZE, BUFFERS) " + SQL_Q2, (SLUG, MIN_BUDGET))
        print("\n--- PostgreSQL, план Q2 ---")
        for (line,) in c.fetchall():
            print("  " + line)
    ctx.close()

    db = MongoClient(MONGO_URL)["freelancer_lab2"]
    plan = db.command("explain", {
        "find": "project_cards",
        "filter": {"status": "OPEN", "category.slug": SLUG,
                   "pricing.max": {"$gte": MIN_BUDGET}},
        "sort": {"createdAt": -1}, "limit": 20,
    }, verbosity="executionStats")
    st = plan["executionStats"]
    print("--- MongoDB, executionStats Q2 ---")
    print(f"  executionTimeMillis={st['executionTimeMillis']}  "
          f"keysExamined={st['totalKeysExamined']}  docsExamined={st['totalDocsExamined']}  "
          f"returned={st['nReturned']}")


def cleanup():
    """Прибирає тестові записи Q5 і синхронізує лічильник картки."""
    with psycopg.connect(PG_URL, autocommit=True) as pg:
        with pg.cursor() as c:
            c.execute("DELETE FROM lab2.proposal WHERE pr_uid >= %s", (WRITE_BASE,))
    db = MongoClient(MONGO_URL)["freelancer_lab2"]
    db.proposals.delete_many({"_id": {"$gte": WRITE_BASE}})
    db.project_cards.update_one(
        {"_id": PROBE_PID},
        {"$set": {"stats.proposals": db.proposals.count_documents({"p_uid": PROBE_PID})}})


def main():
    """Серія замірів → CSV → плани → прибирання."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=20000)
    ap.add_argument("--reps", type=int, default=30)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--out", default=str(ROOT / "bench" / "results.csv"))
    args = ap.parse_args()

    with psycopg.connect(PG_URL) as pg:
        pid = pg.execute("SELECT p_uid FROM lab2.v_project_card ORDER BY p_uid LIMIT 1").fetchone()[0]

    print(f"Масштаб {args.scale}, повторів {args.reps}, конкуренція {args.concurrency}")
    print("Перевірка ідентичності результатів:")
    if not verify(pid):
        print("ПОМИЛКА: набори даних не збігаються, бенчмарк не запущено.")
        return

    rows = []
    for name, sfn, mfn, mvfn in CASES:
        variants = [("postgresql", "sql", sfn), ("mongodb", "mongodb", mfn)]
        if mvfn:
            variants.append(("postgresql-mv", "sql", mvfn))
        for store_label, store, fn in variants:
            lat = measure(store, fn, pid, args.reps, args.concurrency)
            rec = {
                "scale": args.scale, "concurrency": args.concurrency, "query": name,
                "store": store_label, "reps": len(lat),
                "p50_ms": round(percentile(lat, 50), 3),
                "p95_ms": round(percentile(lat, 95), 3),
                "p99_ms": round(percentile(lat, 99), 3),
                "mean_ms": round(sum(lat) / len(lat), 3),
                "ops_per_sec": round(1000 / (sum(lat) / len(lat)), 1),
            }
            rows.append(rec)
            print(f"  {name:26s} {store_label:16s} "
                  f"p50={rec['p50_ms']:8.3f}  p95={rec['p95_ms']:8.3f}  "
                  f"p99={rec['p99_ms']:8.3f} мс")

    with open(args.out, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if f.tell() == 0:
            w.writeheader()
        w.writerows(rows)
    plans(pid)
    cleanup()
    print(f"\nРезультати дописано: {args.out}")


if __name__ == "__main__":
    main()
