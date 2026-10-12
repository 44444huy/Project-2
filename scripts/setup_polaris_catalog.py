"""Create the Polaris catalog used by the product-review lakehouse."""

from __future__ import annotations

import base64
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


BASE_URL = os.environ["POLARIS_BASE_URL"]
REALM = os.environ["POLARIS_REALM"]
CLIENT_ID = os.environ["POLARIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]
CATALOG_NAME = os.environ["POLARIS_CATALOG_NAME"]
CATALOG_ROLE = os.environ["POLARIS_CATALOG_ROLE"]
WAREHOUSE = os.environ["POLARIS_WAREHOUSE"]
S3_ENDPOINT = os.environ["POLARIS_S3_ENDPOINT"]
S3_REGION = os.environ["POLARIS_S3_REGION"]


def request_json(method, path, headers=None, payload=None, form=None):
    request_headers = dict(headers or {})
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    elif form is not None:
        data = urlencode(form).encode("utf-8")
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"

    request = Request(
        f"{BASE_URL}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    with urlopen(request, timeout=10) as response:
        body = response.read()
        return json.loads(body.decode("utf-8")) if body else None


def wait_for_access_token():
    basic = base64.b64encode(
        f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
    ).decode("ascii")
    for attempt in range(1, 31):
        try:
            response = request_json(
                "POST",
                "/api/catalog/v1/oauth/tokens",
                headers={
                    "Authorization": f"Basic {basic}",
                    "Polaris-Realm": REALM,
                },
                form={
                    "grant_type": "client_credentials",
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
            )
            return response["access_token"]
        except (HTTPError, URLError, TimeoutError, KeyError) as error:
            if attempt == 30:
                raise RuntimeError(
                    "Polaris token endpoint did not become ready"
                ) from error
            print(f"Waiting for Polaris API ({attempt}/30): {error}")
            time.sleep(2)
    raise RuntimeError("Polaris token endpoint did not become ready")


def main():
    token = wait_for_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Polaris-Realm": REALM,
    }
    catalogs_response = request_json(
        "GET", "/api/management/v1/catalogs", headers=headers
    )
    catalog_names = {
        catalog["name"]
        for catalog in (catalogs_response or {}).get("catalogs", [])
    }

    if CATALOG_NAME not in catalog_names:
        request_json(
            "POST",
            "/api/management/v1/catalogs",
            headers=headers,
            payload={
                "catalog": {
                    "name": CATALOG_NAME,
                    "type": "INTERNAL",
                    "readOnly": False,
                    "properties": {"default-base-location": WAREHOUSE},
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [WAREHOUSE],
                        "endpoint": S3_ENDPOINT,
                        "endpointInternal": S3_ENDPOINT,
                        "pathStyleAccess": True,
                        "region": S3_REGION,
                    },
                }
            },
        )
        print(f"Created Polaris catalog: {CATALOG_NAME}")
    else:
        print(f"Polaris catalog already exists: {CATALOG_NAME}")

    grant_path = (
        "/api/management/v1/catalogs/"
        f"{quote(CATALOG_NAME, safe='')}/catalog-roles/"
        f"{quote(CATALOG_ROLE, safe='')}/grants"
    )
    try:
        request_json(
            "PUT",
            grant_path,
            headers=headers,
            payload={
                "type": "catalog",
                "privilege": "CATALOG_MANAGE_CONTENT",
            },
        )
    except HTTPError as error:
        if error.code != 409:
            raise
        print("Catalog privilege already exists")
    else:
        print("Ensured catalog_admin has CATALOG_MANAGE_CONTENT")


if __name__ == "__main__":
    main()
