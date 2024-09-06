import logging
import math
import time
from itertools import chain
from typing import List, Optional

from botocore.client import BaseClient
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class RedshiftQueryException(Exception):
    def __init__(self, id_: str, status: str, error: str) -> None:
        super().__init__(f"query [{id_}] failed status [{status}] with error [{error}]")
        self.error = error
        self.status = status
        self.id = id_


class RedshiftDataClient:
    def __init__(
        self,
        database: str,
        client: BaseClient,
        workgroup: Optional[str] = None,
        secret_arn: Optional[str] = None,
        user: Optional[str] = None,
        cluster_identifier: Optional[str] = None,
    ) -> None:
        self._connection_details = {
            k: v
            for k, v in {
                "WorkgroupName": workgroup,
                "SecretArn": secret_arn,
                "DbUser": user,
                "ClusterIdentifier": cluster_identifier,
            }.items()
            if v
        }
        self.database = database
        self.client = client

    def execute_sqls(self, sqls: List[str], wait: bool = True, timeout_seconds: int = 120) -> str:
        assert (
            len(sqls) <= 40
        ), "Cannot execute more than 40 queries in one call, use execute_sqls_batched instead"
        while True:
            try:
                args = {**self._connection_details, "Database": self.database, "Sqls": sqls}
                resp = self.client.batch_execute_statement(**args)
                log.info("queued [%s]", sqls)
                id_ = resp["Id"]
                print(f"executing {sqls}")
                break
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "ActiveStatementsExceededException":
                    # we hit a query limit ( should be around 200 )
                    log.debug("got ActiveStatementsExceededException, sleeping for 10 sec")
                    if timeout_seconds < 0:
                        raise RuntimeError(
                            "timed out while trying to execute query [ActiveStatementsExceededException]"
                        )
                    timeout_seconds -= 10
                    time.sleep(10)
                    continue
                raise ex
        if wait:
            self._wait(id_, timeout_seconds, 1)
        return id_

    def execute_sql(self, sql, wait: bool = True, timeout_seconds: int = 120) -> str:
        return self.execute_sqls(sqls=[sql], wait=wait, timeout_seconds=timeout_seconds)

    def row_count(self, id_: str):
        resp = self.client.describe_statement(Id=id_)
        if resp["SubStatements"]:
            return resp["SubStatements"][-1]["ResultRows"]
        return resp["ResultRows"]

    def result_set(self, id_: str):
        """Results columnMetadata and an iterator for the rows as tuples"""
        resp = self.client.describe_statement(Id=id_)
        if resp["SubStatements"]:
            id_ = resp["SubStatements"][-1]["Id"]
        paginator = self.client.get_paginator("get_statement_result")
        pages = iter(paginator.paginate(Id=id_))
        # get first page
        first_page = next(pages)

        def iterator():
            for page in chain([first_page], pages):
                for row in page["Records"]:
                    yield tuple(
                        None if "isNull" in column else next(iter(column.values()))
                        for column in row
                    )

        return first_page["ColumnMetadata"], iterator()

    def _wait(self, id_: str, timeout_seconds: int = 120, check_internal_seconds=2):
        total = 0
        sleep_time = 0.1
        assert check_internal_seconds < sleep_time, f"check_internal_seconds must be at least {sleep_time}"
        while total < timeout_seconds:
            resp = self.client.describe_statement(Id=id_)
            status = resp["Status"].upper()
            # happy scenario first
            if status == "FINISHED":
                # success
                return resp.get("ResultsRows", 0)
            if status in ("SUBMITTED", "PICKED", "STARTED"):
                # in progress
                time.sleep(sleep_time)
                total += sleep_time
                # increase sleep time 
                sleep_time = min(sleep_time*2,check_internal_seconds)
                continue
            if status in ("ABORTED", "FAILED"):
                raise RedshiftQueryException(id_, status, resp["Error"])

            raise RuntimeError(f"query [{id_}] invalid status [{status}]")
        # timeout
        # cancel the statement if possible, ignoring all errors
        # if the query is actually successfull the cancel will be an error... so w/e
        try:
            self.client.cancel_statement(Id=str)
        except Exception:
            pass
        raise RuntimeError(f"query [{id_}] timed out after [{total}]")
