# Copyright 2025 Amazon Web Services,, Inc. or its affiliates. All Rights Reserved.
# This AWS Content is provided subject to the terms of the AWS Customer Agreement available at
# http://aws.amazon.com/agreement or other written agreement between Customer and either
# Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.

import boto3
import logging
from collections import namedtuple
from datetime import datetime, timedelta
import pendulum
import time
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
import json as _json
import pendulum as _pendulum
from airflow.utils.state import State as _State
import os

# Note: This import is failing even with "apache-airflow-providers-ssh". If you need SSHOperation later on, try something else.
# from airflow.providers.ssh.operators.ssh import SSHOperator

DmsTasks = namedtuple("DmsTasks", ["head", "tail"])

KST = pendulum.timezone("Asia/Seoul")

AWS_REGION = "ap-northeast-2"
DMS_LOCAL_VERSION = "v3"
SNS_TOPIC_ARN = Variable.get("SNS_ALARM_TOPIC_ARN")
PRJ_STAGE = Variable.get("PROJECT_STAGE")
CUSTOMER = Variable.get("CUSTOMER")
OWNER_NAME = Variable.get("OWNER_NAME")
OWNER_EMAIL = Variable.get("OWNER_EMAIL")
STAGE_L = PRJ_STAGE.lower()
STAGE_U = PRJ_STAGE.upper()
START_DATE_REF = [2025, 7, 1]
GLUE_JOB_PREFIX = f"{CUSTOMER}-dap-{STAGE_L}-glue-job"
GLUE_ROLE = f"{CUSTOMER}-dap-{STAGE_L}-GLUE-ROLE-GLUE"
BATCH_JOB_QUEUE = f"{CUSTOMER}-dap-{STAGE_U}-DEP-BATCH-JOB-QUEUE"
BATCH_JOB_DEFINITION_PREFIX = f"{CUSTOMER}-dap-{STAGE_U}-DEP-BATCH-JOB-DEFINITION"

ssm = boto3.client("ssm", region_name=AWS_REGION)
sts_client = boto3.client("sts")
response = sts_client.get_caller_identity()
AWS_ACCOUNT_ID = response["Account"]


def on_failure_callback(context):
    """
    Callback function executed when an Airflow task fails.
    Sends SNS notification with failure details.

    @param context: Airflow task context containing DAG and task information
    @type context: dict
    @return: Result of SNS publish operation
    @rtype: Any
    """
    now = (datetime.now(KST)).strftime("%Y-%m-%d %H:%M:%S")
    dag_id = str(context["dag"]).replace("<", "").replace(">", "").split(":")[1]
    task_id = str(context["task"]).replace("<", "").replace(">", "").split(":")[1]
    subject = "[Airflow alert] DAG : " + dag_id + " 작업 실패"

    messageArr = [
        "[Airflow alert] DAG : " + dag_id + " 작업 실패",
        "",
        "DAG : " + dag_id,
        "Task : " + task_id,
        "Time : " + now,
        "error : " + str(context["exception"]).replace("\r\n", "<br>　　　"),
        "관리자에게 문의해주세요",
    ]

    publish = SnsPublishOperator(
        task_id="publish_message",
        target_arn=SNS_TOPIC_ARN,
        subject=subject,
        message="\n".join(messageArr),
        aws_conn_id="aws_default",
    )
    return publish.execute(context)


def create_glue_crawler_task(
    task_id: str, crawler_name: str, level: str, retry: bool = False
) -> GlueCrawlerOperator:
    """
    Creates a Glue Crawler task operator with standardized naming convention.

    @param task_id: Unique identifier for the Airflow task
    @type task_id: str
    @param crawler_name: Name of the Glue crawler (will be prefixed with project info)
    @type crawler_name: str
    @param level: Data processing level (e.g., 'raw', 'processed', 'curated')
    @type level: str
    @param retry: Whether to enable retry with extended settings (default: False)
    @type retry: bool
    @return: Configured Glue Crawler operator
    @rtype: GlueCrawlerOperator
    """
    retry_set = [20, 30] if retry else [0, 0]
    return GlueCrawlerOperator(
        task_id=task_id,
        config={"Name": f"{CUSTOMER}_dap_{STAGE_L}_{level}_crawler_{crawler_name}"},
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        retries=retry_set[0],
        retry_delay=timedelta(seconds=retry_set[1]),
    )


def create_glue_job_task(
    task_id: str, job_name: str, retry: bool = False
) -> GlueJobOperator:
    """
    Creates a Glue Job task operator with standardized configuration.

    @param task_id: Unique identifier for the Airflow task
    @type task_id: str
    @param job_name: Name of the Glue job (will be prefixed with project info)
    @type job_name: str
    @param retry: Whether to enable retry with extended settings (default: False)
    @type retry: bool
    @return: Configured Glue Job operator
    @rtype: GlueJobOperator
    """
    retry_set = [20, 60] if retry else [0, 0]
    return GlueJobOperator(
        task_id=task_id,
        job_name=f"{job_name}",
        region_name=AWS_REGION,
        iam_role_name=GLUE_ROLE,
        retries=retry_set[0],
        retry_delay=timedelta(seconds=retry_set[1]),
    )


def create_batch_task(task_id: str, job_name: str) -> BatchOperator:
    """
    Creates an AWS Batch task operator with project-specific configuration.

    @param task_id: Unique identifier for the Airflow task
    @type task_id: str
    @param job_name: Name of the Batch job (will be used for job definition lookup)
    @type job_name: str
    @return: Configured Batch operator
    @rtype: BatchOperator
    """
    return BatchOperator(
        task_id=task_id,
        job_name=f"{job_name}_from_BatchOperator",
        job_queue=BATCH_JOB_QUEUE,
        job_definition=f"{BATCH_JOB_DEFINITION_PREFIX}-{job_name}",
    )


def get_dms_task_arn(task_name):
    """
    Retrieves DMS replication task ARN by task name.

    @param task_name: Name of the DMS replication task. You may or may not include DMS_LOCAL_VERSION postfix. This will auto detect for you.
    @type task_name: str
    @return: ARN of the DMS replication task
    @rtype: str
    @raises ValueError: When DMS task with specified name is not found
    """
    logger = logging.getLogger(__name__)
    dms_hook = DmsHook(aws_conn_id="aws_default")
    dms_client = dms_hook.get_conn()

    try:
        response = dms_client.describe_replication_tasks(
            Filters=[
                {
                    "Name": "replication-task-id",
                    "Values": [task_name],
                }
            ]
        )

        if response["ReplicationTasks"]:
            return response["ReplicationTasks"][0]["ReplicationTaskArn"]
    except dms_client.exceptions.ResourceNotFoundFault as rnff:
        logger.debug(
            f"Task not found with name: {task_name}. Trying with version postfix"
        )
        logger.debug(rnff)

    response = dms_client.describe_replication_tasks(
        Filters=[
            {
                "Name": "replication-task-id",
                "Values": [task_name + "-" + DMS_LOCAL_VERSION],
            }
        ]
    )

    if response["ReplicationTasks"]:
        return response["ReplicationTasks"][0]["ReplicationTaskArn"]
    else:
        raise ValueError(
            f"DMS task with name ({task_name}) or ({task_name}-{DMS_LOCAL_VERSION}) not found"
        )


def _wait_for_dms_completion(task_arn: str, **context):
    """
    Waits for DMS replication task to complete.

    @param task_arn: ARN of the DMS replication task
    @type task_arn: str
    """
    DMS_WAIT_DURATION_SEC = 5
    logger = logging.getLogger(__name__)
    dms_hook = DmsHook(aws_conn_id="aws_default")
    dms_client = dms_hook.get_conn()

    while True:
        response = dms_client.describe_replication_tasks(
            Filters=[
                {
                    "Name": "replication-task-arn",
                    "Values": [task_arn],
                }
            ]
        )

        if not response["ReplicationTasks"]:
            raise ValueError(f"DMS task with ARN {task_arn} not found")

        status = response["ReplicationTasks"][0]["Status"]
        logger.info(f"DMS task status: {status}")

        if status in ["stopped", "failed", "ready"]:
            if status == "failed":
                raise Exception(f"DMS task failed: {task_arn}")
            break

        time.sleep(DMS_WAIT_DURATION_SEC)


def run_dms_task(task_id: str, task_name: str, blocking: bool = True):
    """
    Runs a DMS replication task by starting it with appropriate configuration.
    Automatically determines the appropriate start type based on task status.

    @param task_id: Unique identifier for the Airflow task
    @type task_id: str
    @param task_name: Name of the DMS replication task to start
    @type task_name: str
    @param blocking: Whether to wait for task completion (default: False)
    @type blocking: bool
    @return: Named tuple with head and tail operators for task chaining
    @rtype: DmsTasks
    @raises ValueError: When DMS task with specified name is not found
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating DmsStartTaskOperator with task_id: {task_id}")

    task_arn = get_dms_task_arn(task_name)
    start_type = "reload-target"

    start_task = DmsStartTaskOperator(
        task_id=task_id,
        replication_task_arn=task_arn,
        start_replication_task_type=start_type,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    if not blocking:
        return DmsTasks(head=start_task, tail=start_task)

    wait_task = PythonOperator(
        task_id=f"{task_id}_wait",
        python_callable=_wait_for_dms_completion,
        op_kwargs={"task_arn": task_arn},
    )

    start_task >> wait_task

    return DmsTasks(head=start_task, tail=wait_task)


DEFAULT_ARGS = {
    "owner": OWNER_NAME,
    "depends_on_past": False,
    "email": [OWNER_EMAIL],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

def run_glue_crawler_task(
    task_id: str, crawler_name: str, blocking: bool = True
):
    """
    Runs a Glue Crawler task safely without using ARN.

    @param task_id: Unique identifier for the Airflow task
    @param crawler_name: Name of the Glue crawler
    @param level: Data processing level (e.g., 'raw', 'processed', 'curated')
    @param blocking: Whether to wait for crawler completion (default: True)
    @return: Named tuple with head and tail operators for task chaining
    """
    Tasks = namedtuple("Tasks", ["head", "tail"])

    # Glue Crawler 이름 표준화

    # GlueCrawlerOperator 생성
    glue_crawler_task = GlueCrawlerOperator(
        task_id=task_id,
        config={"Name": crawler_name},
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=blocking,
        retries=0,
        retry_delay=timedelta(seconds=0),
    )

    # blocking=False면 head=tail 동일
    if not blocking:
        return Tasks(head=glue_crawler_task, tail=glue_crawler_task)

    # blocking=True면 tail을 PythonOperator로 Crawler 상태 체크
    # 실제로 wait_for_completion=True면 Operator 자체가 완료까지 기다리므로
    # tail을 glue_crawler_task 자체로 해도 무방
    return Tasks(head=glue_crawler_task, tail=glue_crawler_task)

def run_glue_job_task(task_id: str, job_name: str, blocking: bool = True):
    """
    Runs a Glue Job task safely with optional blocking until completion.

    @param task_id: Unique identifier for the Airflow task
    @param job_name: Glue Job name (will be prefixed with project info)
    @param blocking: Whether to wait for Glue Job completion (default: True)
    @return: Named tuple with head and tail operators for task chaining
    """
    Tasks = namedtuple("Tasks", ["head", "tail"])

    # full_job_name = f"{GLUE_JOB_PREFIX}-{job_name}"

    # GlueJobOperator 생성
    glue_job_task = GlueJobOperator(
        task_id=task_id,
        job_name=job_name,
        region_name=AWS_REGION,
        iam_role_name=GLUE_ROLE,
        retries=0,
        retry_delay=timedelta(seconds=0),
    )

    if not blocking:
        return Tasks(head=glue_job_task, tail=glue_job_task)

    wait_task = GlueJobSensor(
        task_id=f"{task_id}_wait",
        job_name=job_name,
        run_id=glue_job_task.output,       
        verbose=False,
    )

    glue_job_task >> wait_task

    return Tasks(head=glue_job_task, tail=wait_task)


def _wait_for_glue_job_completion(job_name: str, run_id: str, **context):
    """
    Waits for Glue job run to complete.
    
    @param job_name: Name of the Glue job
    @param run_id: Glue Job run ID
    """
    import time
    import boto3
    logger = logging.getLogger(__name__)
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        status = response["JobRun"]["JobRunState"]
        logger.info(f"Glue Job '{job_name}' (RunId: {run_id}) status: {status}")

        if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            if status != "SUCCEEDED":
                raise Exception(f"Glue Job failed with status: {status}")
            break

        time.sleep(10)

# ─────────────────────────────────────────────────────────────────────────────
# Lambda 실행 유틸 (blocking=wait 동작 포함)
# ─────────────────────────────────────────────────────────────────────────────
import json
from typing import Optional
from botocore.config import Config

DEFAULT_LAMBDA_TIMEOUT = 800  # 10분 기본값 

def _invoke_lambda(function_name: str,
                   payload: Optional[dict] = None,
                   invocation_type: str = "RequestResponse",
                   log_type: str = "Tail"):
    """
    Invoke AWS Lambda.
    - RequestResponse: 동기 호출(기다림)
    - Event          : 비동기 호출(바로 반환)

    Raises Exception on FunctionError or non-2xx status.
    Returns parsed payload (dict or str) or status code.
    """
    logger = logging.getLogger(__name__)
    
    lambda_client = boto3.client("lambda", 
                                 region_name=AWS_REGION,
                                 config=Config(
                                    read_timeout=DEFAULT_LAMBDA_TIMEOUT,
                                    connect_timeout=10),
                                 )

    # Lambda Invoke
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=invocation_type,  # "RequestResponse" or "Event"
        LogType=log_type,                # "Tail"이면 응답에 로그 tail(base64) 포함
        Payload=(json.dumps(payload or {})).encode("utf-8"),
    )

    status = response.get("StatusCode", 0)
    func_err = response.get("FunctionError")

    # 동기 호출일 때만 결과 페이로드 읽음
    if invocation_type == "RequestResponse":
        # Payload는 StreamingBody
        parsed = ""
        try:
            body_bytes = response["Payload"].read() if "Payload" in response else b""
            body_text = body_bytes.decode("utf-8") if body_bytes else ""
            parsed = json.loads(body_text) if body_text else None
        except Exception:
            parsed = body_text  # JSON이 아니면 원문 문자열 반환

        # 에러 처리
        if func_err:
            logger.error("Lambda FunctionError: %s", func_err)
            logger.error("Lambda payload: %s", body_text)
            raise Exception(f"Lambda FunctionError: {func_err}")

        if status < 200 or status >= 300:
            logger.error("Lambda status code not OK: %s", status)
            logger.error("Lambda payload: %s", body_text)
            raise Exception(f"Lambda invoke failed, status={status}")

        # 정상 리턴
        logger.info("Lambda invoke succeeded (status=%s)", status)
        return parsed

    # 비동기 호출일 경우 상태코드만 반환
    logger.info("Lambda async(Event) invoke submitted (status=%s)", status)
    return status


def run_lambda_task(task_id: str,
                    function_name: str,
                    payload: Optional[dict] = None,
                    blocking: bool = True):
    """
    Airflow에서 Lambda 실행 태스크를 생성.
    - blocking=True  → 동기 호출(RequestResponse), 완료까지 기다림 (== wait)
    - blocking=False → 비동기(Event) 호출, 바로 다음 태스크로 진행

    반환: namedtuple("Tasks", ["head", "tail"])
    """
    Tasks = namedtuple("Tasks", ["head", "tail"])

    # blocking=True일 때는 동기 호출로 완료까지 기다리는 PythonOperator 하나로 충분
    if blocking:
        py = PythonOperator(
            task_id=task_id,
            python_callable=_invoke_lambda,
            op_kwargs={
                "function_name": function_name,
                "payload": payload or {},
                "invocation_type": "RequestResponse",
                "log_type": "Tail",
            },
        )
        return Tasks(head=py, tail=py)

    # blocking=False → 비동기 호출(Event)로 제출만
    py_async = PythonOperator(
        task_id=task_id,
        python_callable=_invoke_lambda,
        op_kwargs={
            "function_name": function_name,
            "payload": payload or {},
            "invocation_type": "Event",
            "log_type": "None",
        },
    )
    return Tasks(head=py_async, tail=py_async)


# ─────────────────────────────────────────────────────────────────────────────
# 공통 알림(Notifications) - SNS only / Pretty Text / 표준 콜백
# ─────────────────────────────────────────────────────────────────────────────

# Airflow Variable → ENV → 기본값 순으로 설정값 조회
def _get_tag(name: str, default: str | None = None):
    try:
        v = Variable.get(name)
        if v:
            return v
    except Exception:
        pass
    return os.environ.get(name, default)

# 표준 태그(계정/환경/플랫폼/정책버전)
_NOTIFY_ACCOUNT  = _get_tag("ACCOUNT_ALIAS",  "unknown-acct")
_NOTIFY_ENV      = _get_tag("ENV",            "dev")
_NOTIFY_PLATFORM = _get_tag("PLATFORM",       "dap")
_NOTIFY_POLICY   = _get_tag("POLICY_VERSION", "v1")

def _now_iso_utc() -> str:
    return _pendulum.now("UTC").to_datetime_string()

def _format_pretty_message(
    title: str,
    fields: dict,
    log_url: str | None = None,
    exception: str | None = None,
    trailer_json: dict | None = None,
    failed_tasks: list[dict] | None = None,
) -> str:
    """SNS 이메일 본문을 사람이 읽기 좋은 텍스트로 포맷"""
    lines: list[str] = []
    lines.append("=" * 46)
    lines.append(f"[ {title} ]")
    lines.append("=" * 46)
    lines.append(
        f"Account={_NOTIFY_ACCOUNT} | Env={_NOTIFY_ENV} | "
        f"Platform={_NOTIFY_PLATFORM} | Policy={_NOTIFY_POLICY}"
    )
    lines.append("-" * 46)

    # Key:Value 표 (좌측 18칸 고정)
    for k, v in fields.items():
        if v is None:
            continue
        key = str(k)[:18]
        val = str(v)
        lines.append(f"{key:18}: {val}")

    # 실패 Task 목록
    if failed_tasks:
        lines.append("\n--- Failed Tasks ---")
        for ft in failed_tasks:
            bullet = (
                f"• {ft.get('task_id')} "
                f"(try={ft.get('try_number')}, state={ft.get('state')}, "
                f"dur={ft.get('duration') or '-'}s"
            )
            if ft.get("map_index") not in (None, -1):
                bullet += f", map={ft['map_index']}"
            bullet += ")"
            lines.append(bullet)
            if ft.get("log_url"):
                lines.append(f"  ↳ log: {ft['log_url']}")

    if exception:
        lines.append("\n--- Error ---")
        lines.append(str(exception))

    if log_url:
        lines.append(f"\n▶ 로그 확인: {log_url}")

    lines.append("\n--")
    lines.append(f"Generated at (UTC): {_now_iso_utc()}")

    if trailer_json:
        try:
            pretty = _json.dumps(trailer_json, ensure_ascii=False, indent=2, default=str)
            lines.append("\n--- JSON Payload ---")
            lines.append(pretty)
        except Exception:
            pass

    return "\n".join(lines)

def _sns_publish_text(subject: str, message_text: str):
    """SNS로 텍스트 발송 (이메일 구독 시 본문으로 표시)"""
    if not SNS_TOPIC_ARN:
        logging.getLogger(__name__).info("[SNS SKIP] %s\n%s", subject, message_text)
        return
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=(subject or "Airflow Notification")[:256],  # SNS Subject 제한
            Message=message_text,
        )
        logging.getLogger(__name__).info("[SNS OK] %s", subject)
    except Exception as e:
        logging.getLogger(__name__).exception("[SNS FAIL] %s", e)

def _notify(
    subject: str,
    title: str,
    fields: dict,
    log_url: str | None = None,
    exception: str | None = None,
    json_payload: dict | None = None,
    failed_tasks: list[dict] | None = None,
):
    """예쁜 텍스트 + (하단) JSON 원문 묶어서 SNS 발송"""
    body = _format_pretty_message(
        title=title,
        fields=fields,
        log_url=log_url,
        exception=exception,
        trailer_json=json_payload or fields,
        failed_tasks=failed_tasks,
    )
    _sns_publish_text(subject, body)

def _collect_failed_tis(dag_run) -> list[dict]:
    """FAILED/UPSTREAM_FAILED TaskInstance 목록 수집"""
    failed = []
    try:
        for ti in dag_run.get_task_instances():
            if ti.state in {_State.FAILED, _State.UPSTREAM_FAILED}:
                dur = ti.duration
                if dur is None and ti.start_date and ti.end_date:
                    dur = (ti.end_date - ti.start_date).total_seconds()
                failed.append(
                    {
                        "task_id": ti.task_id,
                        "try_number": ti.try_number,
                        "state": ti.state,
                        "duration": dur,
                        "map_index": getattr(ti, "map_index", None),
                        "log_url": getattr(ti, "log_url", None),
                    }
                )
    except Exception:
        logging.getLogger(__name__).exception("Collecting failed TIs failed")
    return failed

# ── 표준 콜백들 (DAG/Task)
def std_task_failure_alert(context):
    """Task 실패 알림(사람이 읽기 좋은 포맷)"""
    ti = context["ti"]
    subject = f"[Airflow][{_NOTIFY_ENV}] TASK 실패 알림 - {ti.dag_id}.{ti.task_id}"
    fields = {
        "Event":        "Task Failure",
        "DAG":          ti.dag_id,
        "Task":         ti.task_id,
        "Run ID":       context.get("run_id"),
        "Try #":        ti.try_number,
        "Logical Date": str(context.get("logical_date")),
        "Execution":    str(context.get("execution_date")),
        "Owner":        getattr(ti.task, "owner", None),
        "Duration(s)":  ti.duration,
    }
    payload = {
        "event": "task_failure",
        "account": _NOTIFY_ACCOUNT,
        "env": _NOTIFY_ENV,
        "platform": _NOTIFY_PLATFORM,
        "policy": _NOTIFY_POLICY,
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "logical_date": str(context.get("logical_date")),
        "execution_date": str(context.get("execution_date")),
        "map_index": getattr(ti, "map_index", None),
        "owner": getattr(ti.task, "owner", None),
        "log_url": ti.log_url,
        "exception": str(context.get("exception")),
        "duration": ti.duration,
        "notified_at_utc": _now_iso_utc(),
    }
    _notify(
        subject=subject,
        title="TASK 실패 알림",
        fields=fields,
        log_url=ti.log_url,
        exception=str(context.get("exception")),
        json_payload=payload,
    )

def std_dag_failure_alert(context):
    """DAG 실패 알림(실패 Task 목록 포함)"""
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else context.get("dag_id")
    subject = f"[Airflow][{_NOTIFY_ENV}] DAG 실패 알림 - {dag_id}"

    start = getattr(dag_run, "start_date", None)
    end   = getattr(dag_run, "end_date", None)
    duration = (end - start).total_seconds() if (start and end) else None

    failed_list = _collect_failed_tis(dag_run)
    fields = {
        "Event":        "DAG Failure",
        "DAG":          dag_id,
        "Run ID":       context.get("run_id"),
        "Start":        str(start),
        "End":          str(end),
        "Duration(s)":  duration,
        "Failed Count": len(failed_list),
    }
    payload = {
        "event": "dag_failure",
        "account": _NOTIFY_ACCOUNT,
        "env": _NOTIFY_ENV,
        "platform": _NOTIFY_PLATFORM,
        "policy": _NOTIFY_POLICY,
        "dag_id": dag_id,
        "run_id": context.get("run_id"),
        "start_date": str(start),
        "end_date": str(end),
        "duration_sec": duration,
        "failed_tasks": failed_list,
        "notified_at_utc": _now_iso_utc(),
    }

    # DAG 레벨 log_url은 없을 수 있음
    log_url = None
    try:
        if "ti" in context and hasattr(context["ti"], "log_url"):
            log_url = context["ti"].log_url
    except Exception:
        pass

    _notify(
        subject=subject,
        title="DAG 실패 알림",
        fields=fields,
        log_url=log_url,
        exception=None,
        json_payload=payload,
        failed_tasks=failed_list,
    )

def std_dag_success_alert(context):
    """DAG 성공 알림"""
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else context.get("dag_id")
    subject = f"[Airflow][{_NOTIFY_ENV}] DAG 성공 알림 - {dag_id}"

    start = getattr(dag_run, "start_date", None)
    end   = getattr(dag_run, "end_date", None)
    duration = (end - start).total_seconds() if (start and end) else None

    fields = {
        "Event":        "DAG Success",
        "DAG":          dag_id,
        "Run ID":       context.get("run_id"),
        "Start":        str(start),
        "End":          str(end),
        "Duration(s)":  duration,
    }
    payload = {
        "event": "dag_success",
        "account": _NOTIFY_ACCOUNT,
        "env": _NOTIFY_ENV,
        "platform": _NOTIFY_PLATFORM,
        "policy": _NOTIFY_POLICY,
        "dag_id": dag_id,
        "run_id": context.get("run_id"),
        "start_date": str(start),
        "end_date": str(end),
        "duration_sec": duration,
        "notified_at_utc": _now_iso_utc(),
    }

    _notify(
        subject=subject,
        title="DAG 성공 알림",
        fields=fields,
        log_url=None,
        exception=None,
        json_payload=payload,
    )

# 표준 DEFAULT_ARGS(필요 시 import 해서 그대로 사용)
NOTIFY_DEFAULT_ARGS = {
    "owner": OWNER_NAME,
    "depends_on_past": False,
    "email": [OWNER_EMAIL],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": std_task_failure_alert,  # Task 실패 시 공통 콜백
}


