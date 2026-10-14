"""Five real SDK workflows. Uses disposable tables in an existing database.

Set MO_HOST, MO_PORT, MO_USER, MO_PASSWORD, MO_DATABASE, MO_SSL_MODE.
DuckDB comparisons use its ordinary local Python API, not the S3 extension.
"""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import tempfile
import time
import uuid

import duckdb
from matrixone import Client
import pandas as pd
from sklearn.linear_model import LogisticRegression


def connect():
    client = Client(sql_log_mode="off")
    client.connect(
        host=os.environ["MO_HOST"],
        port=int(os.environ.get("MO_PORT", "6001")),
        user=os.environ["MO_USER"],
        password=os.environ["MO_PASSWORD"],
        database=os.environ["MO_DATABASE"],
        ssl_mode=os.environ.get("MO_SSL_MODE", "required"),
        on_connect=[],  # Analytics examples do not need vector/fulltext hooks.
    )
    return client


def frame(client, sql):
    # Existing SDK API. Not a built-in .df() or .to_pandas() method.
    result = client.execute(sql)
    return pd.DataFrame(result.fetchall(), columns=result.columns)


def equal(left, right):
    pd.testing.assert_frame_equal(left, right, check_dtype=False, atol=1e-8, rtol=1e-8)


def run():
    prefix = "ds_" + uuid.uuid4().hex[:12]
    tables = []
    snapshots = []
    reports = []
    client = connect()
    reader = None
    duck = duckdb.connect()
    database = os.environ["MO_DATABASE"]
    customers = pd.DataFrame([
        {"customer_id": i, "region": ["east", "west", "north"][i % 3],
         "churn": int(i % 2 == 0)}
        for i in range(1, 13)
    ])
    orders = pd.DataFrame([
        {"order_id": i * 10 + j, "customer_id": i, "amount": float(i * 10 + j)}
        for i in range(1, 13) for j in range(1, 1 + (1 if i % 2 == 0 else 3))
    ])
    duck.register("customers", customers)
    duck.register("orders", orders)

    def table(suffix, schema):
        name = prefix + "_" + suffix
        tables.append(name)  # Cleanup also runs after a failed load.
        client.execute(f"CREATE TABLE {name} ({schema})")
        return name

    def upload(name, data):
        # Small-frame bridge; every row is transmitted to MO. No zero-copy claim.
        records = data.astype(object).where(pd.notna(data), None).to_dict("records")
        for start in range(0, len(records), 1000):
            client.batch_insert(name, records[start:start + 1000])

    def record(name, started, **detail):
        item = {"case": name, "status": "passed",
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 3), **detail}
        reports.append(item)
        print(json.dumps(item, ensure_ascii=False), flush=True)

    try:
        customer_table = table("customers", "customer_id BIGINT PRIMARY KEY, region VARCHAR(20), churn INT")
        order_table = table("orders", "order_id BIGINT PRIMARY KEY, customer_id BIGINT, amount DOUBLE")
        upload(customer_table, customers)
        upload(order_table, orders)

        # 1. Notebook exploration: cloud aggregation -> Pandas -> chart-ready result.
        started = time.perf_counter()
        sql = """SELECT c.region, COUNT(*) AS orders, SUM(o.amount) AS revenue
                 FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id
                 GROUP BY c.region ORDER BY c.region"""
        overview = frame(client, sql.format(orders=order_table, customers=customer_table))
        equal(overview, duck.sql(sql.format(orders="orders", customers="customers")).df())
        record("01_notebook_to_pandas", started, rows=len(overview),
               result=overview.to_dict("records"))

        # 2. A local experiment cohort joined to shared database facts.
        started = time.perf_counter()
        cohort = pd.DataFrame({"customer_id": [1, 4, 7, 10],
                               "experiment_group": ["control", "control", "test", "test"]})
        duck.register("cohort", cohort)  # DuckDB queries this Python object directly.
        cohort_table = table("cohort", "customer_id BIGINT PRIMARY KEY, experiment_group VARCHAR(20)")
        upload(cohort_table, cohort)  # Existing MO SDK requires explicit upload + schema.
        sql = """SELECT c.experiment_group, COUNT(*) AS orders, SUM(o.amount) AS revenue
                 FROM {cohort} c JOIN {orders} o ON c.customer_id = o.customer_id
                 GROUP BY c.experiment_group ORDER BY c.experiment_group"""
        joined = frame(client, sql.format(cohort=cohort_table, orders=order_table))
        equal(joined, duck.sql(sql.format(cohort="cohort", orders="orders")).df())
        record("02_local_dataframe_join", started, uploaded_rows=len(cohort),
               result=joined.to_dict("records"))

        # 3. Explore a file that lives on the Python client's machine.
        started = time.perf_counter()
        file_table = table("csv", "order_id BIGINT PRIMARY KEY, customer_id BIGINT, amount DOUBLE")
        with tempfile.TemporaryDirectory(prefix="mo-ds-client-") as directory:
            path = Path(directory) / "orders.csv"
            orders.to_csv(path, index=False)
            # read_csv(path) emits server-side LOAD DATA INFILE. Transfer explicitly.
            # Inline CSV is deliberately limited here to a small, simple fixture.
            csv_payload = pd.read_csv(path).to_csv(index=False, header=False)
            client.load_data.read_csv(csv_payload, table=file_table, inline=True)
            expected = duck.execute(
                "SELECT COUNT(*) AS orders, SUM(amount) AS revenue FROM read_csv_auto(?) WHERE amount >= 50",
                [str(path)],
            ).df()
            actual = frame(client, f"SELECT COUNT(*) AS orders, SUM(amount) AS revenue FROM {file_table} WHERE amount >= 50")
            equal(actual, expected)
        record("03_client_csv_exploration", started, uploaded_rows=len(orders),
               result=actual.to_dict("records"))

        # 4. Atomic incremental refresh, seen through a separate SDK connection.
        started = time.perf_counter()
        reader = connect()
        before = float(reader.execute(f"SELECT SUM(amount) FROM {order_table}").scalar())
        with client.session() as session:
            session.execute(f"UPDATE {order_table} SET amount = amount + 5 WHERE order_id = 11")
            session.batch_insert(order_table, [{"order_id": 999, "customer_id": 1, "amount": 7.0}])
        after = float(reader.execute(f"SELECT SUM(amount) FROM {order_table}").scalar())
        assert after == before + 12, (before, after)
        class RollbackDemo(Exception):
            pass
        try:
            with client.session() as session:
                session.execute(f"UPDATE {order_table} SET amount = amount + 1000 WHERE order_id = 11")
                raise RollbackDemo()
        except RollbackDemo:
            pass
        assert float(reader.execute(f"SELECT SUM(amount) FROM {order_table}").scalar()) == after
        record("04_incremental_update_and_rollback", started, before=before, after=after,
               separate_reader=True, rollback_verified=True)

        # 5. Freeze a training dataset, train locally, then reproduce after a write.
        started = time.perf_counter()
        snapshot = prefix + "_training"
        client.snapshots.create(snapshot, level="database", database=database)
        snapshots.append(snapshot)
        # MO's snapshot modifier pins both tables to the same database snapshot.
        feature_sql = """SELECT c.customer_id, COUNT(o.order_id) AS order_count,
                         COALESCE(SUM(o.amount), 0) AS spend, c.churn
                         FROM {customers} c LEFT JOIN {orders} o
                         ON c.customer_id = o.customer_id
                         GROUP BY c.customer_id, c.churn ORDER BY c.customer_id"""
        frozen_sql = feature_sql.format(
            customers=f"{customer_table} {{snapshot = '{snapshot}'}}",
            orders=f"{order_table} {{snapshot = '{snapshot}'}}",
        )
        training = frame(client, frozen_sql)
        model = LogisticRegression(random_state=0).fit(
            training[["order_count", "spend"]], training["churn"],
        )
        predictions = model.predict(training[["order_count", "spend"]])
        client.execute(f"UPDATE {order_table} SET amount = amount + 100 WHERE customer_id = 1")
        equal(frame(client, frozen_sql), training)
        current = frame(client, feature_sql.format(customers=customer_table, orders=order_table))
        assert float(current["spend"].sum()) > float(training["spend"].sum())
        # This is a plumbing demonstration, not a model-quality evaluation.
        record("05_snapshot_to_sklearn", started, training_rows=len(training),
               predictions=predictions.tolist(), reproducible_after_update=True)

        return {"cases": reports, "all_passed": len(reports) == 5,
                "versions": {package: importlib.metadata.version(package) for package in
                             ["matrixone-python-sdk", "pandas", "duckdb", "scikit-learn", "SQLAlchemy"]},
                "backend": str(client.get_backend_version()),
                "scope": "SDK integration correctness; not a cloud/S3 performance benchmark"}
    finally:
        if reader is not None:
            reader.disconnect()
        cleanup_errors = []
        for snapshot in reversed(snapshots):
            try:
                client.snapshots.delete(snapshot)
            except Exception as error:
                cleanup_errors.append(f"snapshot {snapshot}: {type(error).__name__}")
        for name in reversed(tables):
            try:
                client.execute(f"DROP TABLE IF EXISTS {name}")
            except Exception as error:
                cleanup_errors.append(f"table {name}: {type(error).__name__}")
        client.disconnect()
        duck.close()
        if cleanup_errors:
            raise RuntimeError("Disposable resource cleanup failed: " + "; ".join(cleanup_errors))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    summary = run()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
