from bottle import route, run
from collections import defaultdict
from datetime import datetime, timezone
from dateutil import tz
from dateutil.parser import parse
import itertools
import json
import operator
import redis

REDIS_HOST = "localhost"
REDIS_PORT = int("6379")
REDIS_DB = int("0")
REDIS_NAMESPACES = "resque:crm:development".split(",")


def parse_date(date_string):
    if type(date_string) == bytes:
        date_string = date_string.decode("utf-8")

    return parse(date_string)


def format_date(date):
    return date.isoformat()


def reformat_date(date_string):
    return format_date(parse_date(date_string))


def get_resque_statistics(client, namespace):
    now = datetime.now(timezone.utc)
    workers = defaultdict(dict)
    queues = defaultdict(dict)

    for worker_key in client.smembers(f"{namespace}:workers"):
        worker_key = worker_key.decode("utf-8")

        worker_started_at = parse_date(
            client.get(f"{namespace}:worker:{worker_key}:started")
        )

        workers[worker_key]["started"] = format_date(worker_started_at)
        workers[worker_key]["age"] = (now - worker_started_at).total_seconds()

        worker_heartbeat_at = parse_date(
            client.hget(f"{namespace}:workers:heartbeat", worker_key)
        )

        worker_job_status = json.loads(
            client.get(f"{namespace}:worker:{worker_key}") or "{}"
        )

        if "run_at" in worker_job_status:
            workers[worker_key]["working"] = True
            workers[worker_key]["queue"] = worker_job_status["queue"]
            working_since = parse_date(worker_job_status["run_at"])
            workers[worker_key]["working_since"] = format_date(working_since)
            workers[worker_key]["working_since_time_ago"] = (
                now - working_since
            ).total_seconds()
        else:
            workers[worker_key]["working"] = False
            workers[worker_key]["queue"] = None
            workers[worker_key]["working_since"] = None
            workers[worker_key]["working_since_time_ago"] = None

        workers[worker_key]["heartbeat_at"] = format_date(worker_heartbeat_at)
        workers[worker_key]["heartbeat_time_ago"] = (
            now - worker_heartbeat_at
        ).total_seconds()

        workers[worker_key]["jobs_processed"] = int(
            client.get(f"{namespace}:stat:processed:{worker_key}") or 0
        )
        workers[worker_key]["jobs_failed"] = int(
            client.get(f"{namespace}:stat:failed:{worker_key}") or 0
        )

    for queue in client.smembers(f"{namespace}:queues"):
        queue = queue.decode("utf-8")

        queues[queue]["jobs_pending"] = client.llen(f"{namespace}:queue:{queue}")
        queues[queue]["jobs_failed"] = client.llen(f"{namespace}:failed")

    worker_times_since_idle = [
        worker["working_since_time_ago"]
        for worker in workers.values()
        if worker["working_since_time_ago"]
    ]
    if any(worker_times_since_idle):
        time_since_worker_was_idle = min(worker_times_since_idle)
    else:
        if len(workers) > 0:
            time_since_worker_was_idle = 0
        else:
            time_since_worker_was_idle = None

    return {
        "number_of_workers": len(client.smembers(f"{namespace}:workers")),
        "jobs_failed": int(client.get(f"{namespace}:stat:failed")),
        "jobs_processed": int(client.get(f"{namespace}:stat:processed")),
        "worker_status": workers,
        "workers": len(workers),
        "workers_idle": len(
            [worker for worker in workers.values() if not worker["working"]]
        ),
        "workers_working": len(
            [worker for worker in workers.values() if worker["working"]]
        ),
        "time_since_worker_was_idle": time_since_worker_was_idle,
        "queues": queues,
    }


@route("/")
def index():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    return {
        namespace: get_resque_statistics(client, namespace)
        for namespace in REDIS_NAMESPACES
    }


if __name__ == "__main__":
    run(host="localhost", port="8000", reloader=True)
