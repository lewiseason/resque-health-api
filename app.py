from bottle import route, run
from collections import defaultdict
from datetime import datetime
from dateutil import tz
from dateutil.parser import parse
import itertools
import json
import operator
import os
import re
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))


def parse_date(date_string):
    if type(date_string) == bytes:
        date_string = date_string.decode("utf-8")

    return parse(date_string)


def format_date(date):
    return date.isoformat()


def reformat_date(date_string):
    return format_date(parse_date(date_string))


def get_resque_statistics(client, namespace):
    now = datetime.now().replace(tzinfo=tz.gettz("UTC"))
    workers = defaultdict(dict)
    queues = defaultdict(dict)

    for worker_key in client.smembers(
        "{namespace}:workers".format(namespace=namespace)
    ):
        worker_key = worker_key.decode("utf-8")

        worker_started_at = parse_date(
            client.get(
                "{namespace}:worker:{worker_key}:started".format(
                    namespace=namespace, worker_key=worker_key
                )
            )
        )

        workers[worker_key]["started"] = format_date(worker_started_at)
        workers[worker_key]["age"] = (now - worker_started_at).total_seconds()

        worker_heartbeat_at = parse_date(
            client.hget(
                "{namespace}:workers:heartbeat".format(namespace=namespace), worker_key
            )
        )

        worker_job_status = json.loads(
            client.get(
                "{namespace}:worker:{worker_key}".format(
                    namespace=namespace, worker_key=worker_key
                )
            )
            or "{}"
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
            client.get(
                "{namespace}:stat:processed:{worker_key}".format(
                    namespace=namespace, worker_key=worker_key
                )
            )
            or 0
        )
        workers[worker_key]["jobs_failed"] = int(
            client.get(
                "{namespace}:stat:failed:{worker_key}".format(
                    namespace=namespace, worker_key=worker_key
                )
            )
            or 0
        )

    for queue in client.smembers("{namespace}:queues".format(namespace=namespace)):
        queue = queue.decode("utf-8")

        queues[queue]["jobs_pending"] = client.llen(
            "{namespace}:queue:{queue}".format(namespace=namespace, queue=queue)
        )
        queues[queue]["jobs_failed"] = client.llen(
            "{namespace}:failed".format(namespace=namespace)
        )

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
        "number_of_workers": len(
            client.smembers("{namespace}:workers".format(namespace=namespace))
        ),
        "jobs_failed": int(
            client.get("{namespace}:stat:failed".format(namespace=namespace))
        ),
        "jobs_processed": int(
            client.get("{namespace}:stat:processed".format(namespace=namespace))
        ),
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
    resque_instances = [
        re.sub(r":queues$", "", key.decode("utf-8")) for key in client.keys("*:queues")
    ]

    return {key: get_resque_statistics(client, key) for key in resque_instances}


if __name__ == "__main__":
    run(host="localhost", port="8000", reloader=True)
