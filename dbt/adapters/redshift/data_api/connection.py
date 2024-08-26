from dbt.adapters.redshift.data_api.client import RedshiftDataClient
from typing import Optional, NamedTuple, List, Dict, Any
import boto3
from redshift_connector.utils.oids import RedshiftOID


def _tracer(f):
    def wrapper(*args, **kwargs):
        try:
            print("enter", f.name)
            return f(*args, **kwargs)
        finally:
            print("exit", f.name)

    return wrapper


class Column(NamedTuple):
    name: str
    type_code: int
    display_size: int
    internal_size: int
    precision: int
    scale: int
    null_ok: bool


class Cursor:
    def __init__(self, client: RedshiftDataClient) -> None:
        self._cl = client
        self._described = None
        self._query_id = None
        self._result_set = None
        self._column_metadata: Optional[List[Dict[str, Any]]] = None

    def execute(self, operation, *args, **kwargs):
        self._query_id = self._cl.execute_sql(operation, True)
        return self

    def _iter_result_set(self):
        if self._result_set is None:
            self._get_result_set(self._query_id)
        return self._get_result_set

    def fetchone(self):
        if self._result_set is None:
            self._get_result_set(self._query_id)
        row = next(self._result_set)
        return row

    def fetchall(self):
        if self._result_set is None:
            self._get_result_set(self._query_id)
        return [c for c in self._result_set]

    @property
    def rowcount(self):
        return self._cl.row_count(self._query_id)

    def _get_result_set(self, query_id: str):
        meta, cursor = self._cl.result_set(query_id)
        self._column_metadata = meta
        self._result_set = cursor

    def _get_description(self, query_id: str):
        type_code_map = {
            "LONG": RedshiftOID.BIGINT,
            "DOUBLE": RedshiftOID.DECIMAL,
            "STRING": RedshiftOID.VARCHAR,
            "BOOLEAN": RedshiftOID.BOOLEAN,
            "BLOB": RedshiftOID.VARCHAR,
        }
        # need to fetch at least one row
        if self._column_metadata is None:
            self._get_result_set(query_id)
        return [
            # figure this out
            # name # type_code # display_size # internal_size # precision # scale # null_ok
            Column(
                column["name"],
                type_code_map.get(column["typeName"].upper(), type_code_map["STRING"]),
                column["length"],
                column["length"],
                column["precision"],
                column["scale"],
                column["nullable"] == 1,
            )
            for column in (self._column_metadata or ())
        ]

    @property
    def description(self):
        if not self._query_id:
            raise RuntimeError("Must have executed at least one query before")
        metadata = self._get_description(self._query_id)
        return metadata

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class DataApiConnection:
    """Data API is stateless so there is no "connection" """

    def __init__(
        self,
        database: str,
        cluster_identifier: Optional[str],
        db_user: Optional[str] = None,
        workgroup: Optional[str] = None,
        secret_arn: Optional[str] = None,
        iam_role: Optional[str] = None,
        **kwargs,
    ) -> None:
        if iam_role:
            sts = boto3.client("sts")
            account = sts.get_caller_identity()["Account"]
            role_arn = f"arn:aws:iam::{account}:role/{iam_role}"
            creds = sts.assume_role(RoleArn=role_arn, RoleSessionName="dbt-redshift")[
                "Credentials"
            ]
            creds = {
                "aws_access_key_id": creds["AccessKeyId"],
                "aws_secret_access_key": creds["SecretAccessKey"],
                "aws_session_token": creds["SessionToken"],
            }
            sess = boto3.Session(**creds)
            client = sess.client("redshift-data")
        else:
            client = None

        self.cl = RedshiftDataClient(
            database,
            client,
            workgroup=workgroup,
            user=db_user,
            cluster_identifier=cluster_identifier,
            secret_arn=secret_arn,
        )

    def cursor(self):
        return Cursor(self.cl)
