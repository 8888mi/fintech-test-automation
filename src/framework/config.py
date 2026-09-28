"""Typed configuration loaded from environment / .env.

Every knob a test run needs lives here. Nothing reads os.getenv directly
outside this module, which means switching between local, docker and CI is
a matter of environment variables rather than code edits.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    payments_topic: str = "payments.events"

    # Security. Left as PLAINTEXT locally; set to SASL_PLAINTEXT / SASL_SSL
    # against a real broker and the client picks up the credentials below.
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    # How long a consumer waits for an expected event before failing.
    event_timeout_seconds: float = 15.0

    def client_config(self, group_id: str) -> dict[str, object]:
        """Build a confluent_kafka config dict.

        `auto.offset.reset=earliest` plus a throwaway group id gives us
        read-only behaviour: the test consumer never disturbs the offsets of
        real consumer groups on a shared environment.
        """
        config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            config.update(
                {
                    "sasl.mechanism": self.sasl_mechanism,
                    "sasl.username": self.sasl_username,
                    "sasl.password": self.sasl_password,
                }
            )
        return config


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", env_file=".env", extra="ignore")

    base_url: str = "http://localhost:8000"
    timeout_seconds: float = 10.0
    retry_attempts: int = 3


class UiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="UI_", env_file=".env", extra="ignore")

    base_url: str = "http://localhost:8000"
    # Committed defaults must be safe for CI: a GitHub Actions runner has no X
    # server, so a headed launch fails there. Watch the browser locally through
    # .env (gitignored) or --headed / --slowmo instead of changing these.
    headless: bool = True
    slow_mo_ms: int = 0
    default_timeout_ms: int = 10_000


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = Field(default="local")
    api: ApiSettings = Field(default_factory=ApiSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    ui: UiSettings = Field(default_factory=UiSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
