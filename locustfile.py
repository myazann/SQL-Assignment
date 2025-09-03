# locustfile.py
import json, random, string, time
from locust import HttpUser, task, between

CHAT_PATH = "/e2e/chat"
SQL_PATH = "/e2e/sql"

SIMPLE_SQL = [
    "SELECT * FROM sales",
    "SELECT genre, COUNT(*) AS n FROM sales GROUP BY genre ORDER BY n DESC",
    "SELECT title, runtime FROM sales WHERE runtime > 150 ORDER BY runtime DESC",
    "SELECT AVG(metascore) FROM metadata",
    "SELECT s.title, m.studio FROM sales s JOIN metadata m ON s.title=m.title LIMIT 200",
]

def rnd_name():
    import string, random
    return "U-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

class GradioDBUser(HttpUser):
    wait_time = between(0.05, 0.25)

    @task(3)
    def chat(self):
        msg = random.choice([
            "Which table has the highest number of rows and why?",
            "Write a SQL to list top-5 genres by revenue.",
            "Explain how to compute ROI = worldwide_box_office / production_budget.",
            "What's the average runtime by studio?",
        ])
        payload = {
            "message": msg,
            "history": [
                {"role": "user", "content": "Hi"},
                {"role": "assistant", "content": "Hello!"},
            ],
        }

        t0 = time.perf_counter()
        with self.client.post(
            CHAT_PATH,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            name="chat",
            catch_response=True,
        ) as r:
            dt_ms = (time.perf_counter() - t0) * 1000
            if r.status_code != 200:
                r.failure(f"HTTP {r.status_code}: {r.text[:200]}")
            else:
                # Optionally assert JSON shape
                try:
                    _ = r.json().get("output", "")
                    r.success()
                except Exception as e:
                    r.failure(f"Bad JSON after {dt_ms:.1f}ms: {e}")

    @task(2)
    def sql(self):
        payload = {"query": random.choice(SIMPLE_SQL), "limit": 200, "allow_writes": False}
        t0 = time.perf_counter()
        with self.client.post(
            SQL_PATH,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            name="sql",
            catch_response=True,
        ) as r:
            dt_ms = (time.perf_counter() - t0) * 1000
            if r.status_code != 200:
                r.failure(f"HTTP {r.status_code}: {r.text[:200]}")
            else:
                try:
                    j = r.json()
                    # Optional sanity checks so bad responses are marked failures
                    if "rows" in j and isinstance(j["rows"], list):
                        r.success()
                    else:
                        r.failure(f"Unexpected JSON shape after {dt_ms:.1f}ms: {j}")
                except Exception as e:
                    r.failure(f"Bad JSON after {dt_ms:.1f}ms: {e}")
