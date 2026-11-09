"""Базові запити Redis для п. 6: рядки, хеші, лічильники, sorted set, TTL."""

import json
import time

import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Сесія клієнта: рядок JSON із TTL 30 хв.
r.set("fp:session:s17", json.dumps({"userId": 17, "role": "CLIENT"}), ex=1800)
print("SET/GET  →", r.get("fp:session:s17"), "| TTL =", r.ttl("fp:session:s17"), "с")

# Чернетка відгуку: хеш із полями ставки, терміну й супровідного листа.
r.hset("fp:draft:42:7", mapping={"bid": "1200", "days": "5", "letter": "Зроблю"})
print("HSET     →", r.hgetall("fp:draft:42:7"))
r.delete("fp:draft:42:7")

# Лічильник переглядів: атомарні INCR / INCRBY.
r.delete("fp:views:42")
r.incr("fp:views:42")
r.incrby("fp:views:42", 4)
print("INCR     →", r.get("fp:views:42"), "переглядів")

# Рейтинг виконавців: sorted set із ZINCRBY.
r.delete("fp:rating")
r.zadd("fp:rating", {"u17": 120, "u18": 90})
r.zincrby("fp:rating", 15, "u18")
print("ZADD     →", r.zrange("fp:rating", 0, -1, desc=True, withscores=True))

# Rate limiter: фіксоване вікно 60 с, TTL ставимо лише на першому запиті.
minute = int(time.time()) // 60
key = f"fp:rl:u17:{minute}"
count = r.incr(key)
if count == 1:
    r.expire(key, 60)
print("INCR+TTL →", f"запит {count}/10, дозволено:", count <= 10)
r.delete(key)
