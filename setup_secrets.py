"""
One-time setup script: creates the Databricks secret scope and stores the
lakebase URL and any API keys needed for the project. Run this locally (with the Databricks CLI configured) or
from a notebook - never commit the resulting secret value anywhere.

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

w = WorkspaceClient()

w.secrets.create_scope(scope="trip_planner")
w.secrets.put_secret(
    scope="trip_planner",
    key="lakebase-url",
    string_value=getpass.getpass("Paste your lakebase url")
)

w.secrets.put_acl(
    scope="trip_planner",
    principal="users",
    permission=workspace.AclPermission.READ,
)
