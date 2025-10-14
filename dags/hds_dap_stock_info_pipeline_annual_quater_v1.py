import os
from copy import deepcopy
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
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
    
SPECS = [
  ("hds_sap_stock_ingest_annual",  "HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-ANNUAL"),
  ("hds_sap_stock_ingest_quarter", "HDS-DAP-DEV-SYS1-STOCK-INFO-CRAWLER-QUARTER"),
]

@task_group(group_id="l0_ingestion")
def tg_l0() -> Ends:
    g_start = EmptyOperator(task_id="g_start")
    g_end   = EmptyOperator(task_id="g_end")

    heads, tails = [], []
    for task_id, fn in SPECS:
        j = run_lambda_task(task_id=task_id, function_name=fn, blocking=True)
        heads.append(j.head)
        tails.append(j.tail)

    g_start >> heads
    tails >> g_end

    # ✅ 체이닝 편의: 그룹의 대표 시작/끝을 반환
    return Ends(head=g_start, tail=g_end)
    
@task_group(group_id="l0_crawler")
def tg_l0_crawler() -> Ends:
    j0 = run_glue_crawler_task("stock-info-ingest-quater-l0-crawler", 
                               "crwal-dev-fi-external-l0-naveronly", 
                               True)
    return Ends(head=j0.head, tail=j0.tail)


@task_group(group_id="l1_transform")
def tg_l1() -> Ends:
    j0 = run_glue_job_task("l0-to-l1-etl-glue-job", "fi_naver_l0_to_l1_etl", True)
    j1 = run_glue_crawler_task("l1-crawler", "crwal-dev-fi-external-l1", True)

    j0.tail >> j1.head
    return Ends(head=j0.head, tail=j1.tail)

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    schedule_interval="0 16 16 * *",
    start_date=datetime(*START_DATE_REF, 5, 30, tzinfo=KST),
    default_args=DEFAULT_ARGS_STD,
    max_active_runs=1,
    catchup=False,  # ✅ 일반적으로 배치 파이프라인은 catchup=False 권장
    on_success_callback=std_dag_success_alert,
    on_failure_callback=std_dag_failure_alert,
    tags=["hds_dap", "stock_info", "pipeline", "l0-l2", "annual"],
) as dag:
    # DAG 본문
    g0 = tg_l0()
    g0_crawler = tg_l0_crawler()
    g1 = tg_l1()

    g0.tail >> g0_crawler.head
    g0_crawler.tail >> g1.head