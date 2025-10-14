import os
from copy import deepcopy
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.decorators import task_group
from typing import NamedTuple
from common import (
    START_DATE_REF,
    DEFAULT_ARGS,
    run_glue_crawler_task,
    run_glue_job_task,
    run_lambda_task,
    std_task_failure_alert,
    std_dag_failure_alert,
    std_dag_success_alert,
)

# DEFAULT_ARGS의 on_failure_callback(구버전)을 표준 콜백으로 교체
DEFAULT_ARGS_STD = deepcopy(DEFAULT_ARGS)
DEFAULT_ARGS_STD["on_failure_callback"] = std_task_failure_alert

KST = pendulum.timezone("Asia/Seoul")

class Ends(NamedTuple):
    head: object  # BaseOperator
    tail: object
    
@task_group(group_id="l0_ingestion")
def tg_l0() -> Ends:
    j0 = run_lambda_task("stock_info_ingest_annual", 
                         function_name="HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-ANNUAL", 
                         payload=None,
                         blocking=True)
    j1 = run_glue_crawler_task("stock_info_ingest_annual-l0-crawler", 
                               "crwal-dev-fi-external-l0-naveronly", 
                               True)

    j0.tail >> j1.head
    return Ends(head=j0.head, tail=j1.tail)


# @task_group(group_id="l1_transform")
# def tg_l1() -> Ends:
#     j2 = run_glue_job_task("l0-to-l1-etl-glue-job", "hds-dap-dev-glue-job-krx_lo-to-l1-etl-dops", True)
#     j3 = run_glue_crawler_task("l1-crawler", "crwal-dev-hds-krx-l1-dops", True)

#     j2.tail >> j3.head
#     return Ends(head=j2.head, tail=j3.tail)

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    schedule_interval="0 16 16 * *",
    start_date=datetime(*START_DATE_REF, 5, 30, tzinfo=KST),
    default_args=DEFAULT_ARGS_STD,
    max_active_runs=1,
    on_success_callback=std_dag_success_alert,
    on_failure_callback=std_dag_failure_alert,
    tags=["hds_dap", "stock_info", "pipeline", "l0-l2", "annual"],
) as dag:
    # DAG 본문
    g0 = tg_l0()
    # g1 = tg_l1()
    # g2 = tg_l2()
    
    # g0.tail >> g1.head
    # g1.tail >> g2.head