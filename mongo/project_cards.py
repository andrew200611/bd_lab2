"""Документна проєкція картки замовлення."""

import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne

# Спільний конфіг лежить у корені проєкту.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import MONGO_URL, PG_URL

DB_NAME = "freelancer_lab2"

# schemaVersion дозволяє мігрувати форму документа без зупинки застосунку.
CARD_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "schemaVersion", "title", "status", "createdAt",
                     "pricing", "client", "category", "stats"],
        "properties": {
            "title": {"bsonType": "string"},
            "status": {"enum": ["DRAFT", "OPEN", "IN_PROGRESS", "CLOSED", "CANCELLED"]},
            "schemaVersion": {"bsonType": "int"},
            "createdAt": {"bsonType": "date"},
            "pricing": {
                "bsonType": "object",
                "required": ["type", "max", "currency"],
                "properties": {
                    "type": {"enum": ["FIXED", "HOURLY"]},
                    "max": {"bsonType": ["double", "int", "long"]},
                },
            },
        },
    }
}

PROPOSAL_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "p_uid", "bidAmount", "createdAt"],
        "properties": {
            "bidAmount": {"bsonType": ["double", "int", "long"]},
            "createdAt": {"bsonType": "date"},
        },
    }
}


def connect():
    """Підключення до БД проєкції."""
    return MongoClient(MONGO_URL)[DB_NAME]


def setup(db=None):
    """Перестворює колекції з валідаторами й 4 індексами під Q1–Q5."""
    if db is None:
        db = connect()
    db.drop_collection("project_cards")
    db.drop_collection("proposals")
    db.create_collection("project_cards", validator=CARD_VALIDATOR)
    db.create_collection("proposals", validator=PROPOSAL_VALIDATOR)

    db.project_cards.create_index(
        [("status", ASCENDING), ("category.slug", ASCENDING), ("createdAt", DESCENDING)],
        name="ix_feed")
    db.project_cards.create_index(
        [("pricing.max", ASCENDING)], name="ix_budget",
        partialFilterExpression={"status": "OPEN"})
    db.project_cards.create_index(
        [("skills", ASCENDING)], name="ix_skills",
        partialFilterExpression={"status": "OPEN"})
    db.proposals.create_index([("p_uid", ASCENDING)], name="ix_proposal_project")
    print("MongoDB: колекції та 4 індекси створено")


def _plain(value):
    """Decimal → float: BSON не вміє numeric із PostgreSQL."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value
    return value


def import_from_pg(db=None, batch=5000):
    """Ідемпотентно переливає view і відгуки в MongoDB."""
    import psycopg

    if db is None:
        db = connect()
    with psycopg.connect(PG_URL) as pg:
        with pg.cursor(name="cards_stream") as cur:
            cur.itersize = batch
            cur.execute("SELECT p_uid, created_at, deadline, card "
                        "FROM lab2.v_project_card ORDER BY p_uid")
            ops, total = [], 0
            for p_uid, created_at, deadline, card in cur:
                card = _plain(card)
                card["createdAt"] = created_at
                # У BSON немає date: дедлайн тримаємо рядком ISO.
                card["deadline"] = deadline.isoformat() if deadline else None
                ops.append(ReplaceOne({"_id": p_uid}, card, upsert=True))
                if len(ops) == batch:
                    db.project_cards.bulk_write(ops, ordered=False)
                    total += len(ops)
                    ops = []
            if ops:
                db.project_cards.bulk_write(ops, ordered=False)
                total += len(ops)
        print(f"MongoDB: імпортовано карток — {total}")

        with pg.cursor(name="proposals_stream") as cur:
            cur.itersize = batch
            cur.execute("SELECT pr_uid, p_uid, bid_amount, created_at "
                        "FROM lab2.proposal ORDER BY pr_uid")
            ops, total = [], 0
            for pr_uid, p_uid, amount, created_at in cur:
                ops.append(ReplaceOne({"_id": pr_uid}, {
                    "_id": pr_uid, "p_uid": p_uid,
                    "bidAmount": float(amount), "createdAt": created_at,
                }, upsert=True))
                if len(ops) == batch:
                    db.proposals.bulk_write(ops, ordered=False)
                    total += len(ops)
                    ops = []
            if ops:
                db.proposals.bulk_write(ops, ordered=False)
                total += len(ops)
        print(f"MongoDB: імпортовано відгуків — {total}")


def demo(db=None):
    """Базові CRUD-запити й агрегація для п. 3."""
    if db is None:
        db = connect()
    cards = db.project_cards

    # Тимчасова картка: створюється тут і видаляється в кінці.
    probe_id = 9_999_999
    cards.delete_one({"_id": probe_id})
    cards.insert_one({
        "_id": probe_id, "schemaVersion": 2, "title": "Пробне замовлення",
        "status": "OPEN", "createdAt": datetime.now(),
        "pricing": {"type": "FIXED", "min": 100.0, "max": 200.0, "currency": "USD"},
        "client": {"id": 1, "company": "Приклад"}, "category": {"slug": "dev"},
        "skills": ["python"], "stats": {"proposals": 0},
    })
    print("insertOne →", cards.find_one({"_id": probe_id}, {"title": 1})["title"])

    feed = list(cards.find(
        {"status": "OPEN", "pricing.max": {"$gte": 100}},
        {"title": 1, "pricing.max": 1, "_id": 0}
    ).sort("createdAt", -1).limit(3))
    print("find + sort + limit →", feed)

    cards.update_one({"_id": probe_id, "status": "OPEN"},
                     {"$inc": {"stats.proposals": 1}})
    print("updateOne $inc →", cards.find_one({"_id": probe_id})["stats"])

    revenue = list(cards.aggregate([
        {"$match": {"status": "OPEN"}},
        {"$group": {"_id": "$category.slug", "n": {"$sum": 1},
                    "avgBudget": {"$avg": "$pricing.max"}}},
        {"$sort": {"n": -1}}, {"$limit": 5},
    ]))
    print("aggregate $group →", revenue)

    cards.delete_one({"_id": probe_id})
    print("deleteOne → решта карток:", cards.count_documents({}))


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "all"
    database = connect()
    if action in ("setup", "all"):
        setup(database)
    if action in ("import", "all"):
        import_from_pg(database)
    if action in ("demo", "all"):
        demo(database)
