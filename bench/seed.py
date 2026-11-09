"""Генерація даних lab2, оновлення mv і переливання в MongoDB."""

import argparse
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
# Спільний конфіг і проєкція лежать у сусідніх папках без пакета.
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "mongo"))
from config import PG_URL
from project_cards import setup as mongo_setup, import_from_pg
UTC = timezone.utc
START = datetime(2023, 1, 1, tzinfo=UTC)
THREE_YEARS = 3 * 365 * 24 * 3600

ROOT_SLUGS = ["dev", "design", "text", "marketing", "data", "mobile", "admin", "other"]
SKILLS = ["python", "react", "postgres", "figma", "sql", "java", "aws", "docker",
          "typescript", "go", "excel", "seo", "copywriting", "vue", "mongodb",
          "linux", "photoshop", "english", "analytics", "nodejs"]
WORDS = ["розробка", "дизайн", "API", "інтеграція", "дашборд", "текст", "макет",
         "оптимізація", " landing", "парсер", "звіт", "мобільний застосунок",
         "база даних", "автоматизація", "прототип", "аналітика", "переклад"]
TITLES = ["Розробка сервісу", "Оновлення сайту", "Підготовка макетів",
          "Написання текстів", "Аналіз даних", "Інтеграція API", "Рефакторинг"]


def generate(pg, n, rnd):
    """Наповнює 5 таблиць через COPY і оновлює materialized view."""
    clients = max(200, n // 50)
    categories = 40
    # Розподіл категорій нерівномірний (Zipf), як у реальній стрічці.
    weights = [1.0 / (i + 1) ** 0.8 for i in range(categories)]
    cat_ids = list(range(1, categories + 1))
    cat_root = {}

    with pg.cursor() as cur:
        with cur.copy("COPY lab2.currency (cur_uid, iso_code) FROM STDIN") as cp:
            for uid, code in enumerate(["UAH", "USD", "EUR"], 1):
                cp.write_row((uid, code))

        with cur.copy("COPY lab2.category (cat_uid, parent_uid, slug, name) FROM STDIN") as cp:
            for uid, slug in enumerate(ROOT_SLUGS, 1):
                cp.write_row((uid, None, slug, slug.capitalize()))
                cat_root[uid] = slug
        with cur.copy("COPY lab2.category (cat_uid, parent_uid, slug, name) FROM STDIN") as cp:
            for uid in range(len(ROOT_SLUGS) + 1, categories + 1):
                parent = rnd.choice(range(1, len(ROOT_SLUGS) + 1))
                root = cat_root[parent]
                cat_root[uid] = root
                cp.write_row((uid, parent, f"{root}-{uid}", f"{root.capitalize()} {uid}"))

        with cur.copy("COPY lab2.client (cp_uid, email, company_name, country, rating_avg) "
                      "FROM STDIN") as cp:
            for uid in range(1, clients + 1):
                cp.write_row((uid, f"client{uid}@example.com", f"Компанія {uid}",
                              rnd.choice(["UA", "PL", "DE", "US", "GB"]),
                              Decimal(str(round(rnd.uniform(3, 5), 2)))))

        projects, proposals = [], []
        pr_uid = 0
        for p_uid in range(1, n + 1):
            cat = rnd.choices(cat_ids, weights=weights, k=1)[0]
            root = cat_root[cat]
            created = START + timedelta(seconds=rnd.randrange(THREE_YEARS))
            budget_min = Decimal(str(round(rnd.uniform(100, 5000), 2)))
            budget_max = budget_min + Decimal(str(round(rnd.uniform(0, 5000), 2)))
            skills = rnd.sample(SKILLS, rnd.randint(2, 5))
            description = " ".join(rnd.choice(WORDS) for _ in range(rnd.randint(30, 90)))

            # jsonb з різним набором полів залежно від категорії.
            if root in ("dev", "mobile", "data"):
                attrs = {"stack": skills, "repo": rnd.choice([True, False])}
            elif root in ("design",):
                attrs = {"formats": rnd.sample(["fig", "psd", "png", "svg"], 2)}
            elif root in ("text", "marketing"):
                attrs = {"languagePair": rnd.choice(["uk-en", "en-uk", "uk-pl"])}
            else:
                attrs = {"extras": rnd.randint(0, 5)}

            # 2% м'яко видалених — перевірка фільтра is_deleted у Q3/Q4.
            projects.append((
                p_uid, rnd.randint(1, clients), cat, rnd.randint(1, 3),
                f"{rnd.choice(TITLES)} #{p_uid}", description,
                rnd.choice(["FIXED", "HOURLY"]), budget_min, budget_max,
                rnd.choices(["OPEN", "IN_PROGRESS", "CLOSED", "DRAFT", "CANCELLED"],
                            weights=[70, 10, 15, 3, 2], k=1)[0],
                created,
                (created + timedelta(days=rnd.randint(7, 90))).date(),
                rnd.random() < 0.02,
                # COPY не приймає dict для jsonb напряму.
                skills, Jsonb(attrs),
            ))

            for _ in range(rnd.randint(0, 5)):
                pr_uid += 1
                proposals.append((
                    pr_uid, p_uid,
                    Decimal(str(round(rnd.uniform(100, 8000), 2))),
                    created + timedelta(hours=rnd.randint(1, 72)),
                ))

        cols = ("p_uid, cp_uid, cat_uid, cur_uid, title, description, pricing_type, "
                "budget_min, budget_max, status, created_at, deadline, is_deleted, skills, attrs")
        with cur.copy(f"COPY lab2.project ({cols}) FROM STDIN") as cp:
            for row in projects:
                cp.write_row(row)
        with cur.copy("COPY lab2.proposal (pr_uid, p_uid, bid_amount, created_at) FROM STDIN") as cp:
            for row in proposals:
                cp.write_row(row)

    pg.commit()
    with pg.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW lab2.mv_project_card")
        cur.execute("ANALYZE lab2.project")
        cur.execute("ANALYZE lab2.proposal")
    pg.commit()
    return len(projects), len(proposals)


def main():
    """Перестворює схему, генерує дані масштабу й готує MongoDB."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=20000,
                    choices=[1000, 20000, 100000])
    args = ap.parse_args()

    rnd = random.Random(42)
    with psycopg.connect(PG_URL, autocommit=True) as pg:
        with pg.cursor() as cur:
            cur.execute((ROOT / "sql" / "fragment.sql").read_text(encoding="utf-8"))
    with psycopg.connect(PG_URL) as pg:
        np_, nr_ = generate(pg, args.scale, rnd)
    print(f"PostgreSQL: {np_} замовлень, {nr_} відгуків (схема lab2)")

    mongo_setup()
    import_from_pg()
    print(f"Масштаб {args.scale} готовий до бенчмарку.")


if __name__ == "__main__":
    main()
