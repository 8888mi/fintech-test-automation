"""HTTP client used by every API-level test.

Responsibilities kept in one place so tests never touch `requests` directly:
  * one session with connection pooling
  * retry on transient failures only (never on 4xx - those are real results)
  * every request/response attached to the Allure report automatically
"""

from __future__ import annotations

import json
import logging
from typing import Any, TypeVar

import allure
import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from framework.config import ApiSettings

log = logging.getLogger(__name__)

ModelT = TypeVar("ModelT", bound=BaseModel)

# Retry only on statuses that genuinely indicate a transient condition.
# 4xx is a legitimate business answer and must reach the assertion untouched.
_RETRY_STATUSES = (429, 500, 502, 503, 504)


class ApiResponse:
    """Thin wrapper that makes assertions read like sentences."""

    def __init__(self, raw: requests.Response) -> None:
        self.raw = raw

    @property
    def status_code(self) -> int:
        return self.raw.status_code

    def json(self) -> Any:
        return self.raw.json()

    def as_model(self, model_cls: type[ModelT]) -> ModelT:
        """Parse into a pydantic model - fails loudly on contract drift."""
        return model_cls.model_validate(self.raw.json())

    def expect_status(self, expected: int) -> ApiResponse:
        assert self.status_code == expected, (
            f"Expected HTTP {expected}, got {self.status_code}. Body: {self.raw.text[:500]}"
        )
        return self


class ApiClient:
    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings
        self._session = requests.Session()
        retry = Retry(
            total=settings.retry_attempts,
            backoff_factor=0.4,
            status_forcelist=_RETRY_STATUSES,
            allowed_methods=frozenset({"GET", "POST", "PUT", "DELETE"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def request(self, method: str, path: str, **kwargs: Any) -> ApiResponse:
        url = f"{self._settings.base_url.rstrip('/')}{path}"
        kwargs.setdefault("timeout", self._settings.timeout_seconds)

        with allure.step(f"{method.upper()} {path}"):
            if (body := kwargs.get("json")) is not None:
                allure.attach(
                    json.dumps(body, indent=2, default=str),
                    name="request body",
                    attachment_type=allure.attachment_type.JSON,
                )
            log.info("%s %s", method.upper(), url)
            raw = self._session.request(method, url, **kwargs)
            allure.attach(
                raw.text or "<empty>",
                name=f"response {raw.status_code}",
                attachment_type=allure.attachment_type.JSON
                if raw.headers.get("content-type", "").startswith("application/json")
                else allure.attachment_type.TEXT,
            )
            log.info("-> %s in %.0f ms", raw.status_code, raw.elapsed.total_seconds() * 1000)
        return ApiResponse(raw)

    def get(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("POST", path, **kwargs)

    def close(self) -> None:
        self._session.close()
