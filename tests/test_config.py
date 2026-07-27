from pydantic import SecretStr

from vk_collector.config import Settings


def test_database_password_is_url_encoded_and_hidden() -> None:
    settings = Settings(
        postgres_password=SecretStr("secret:@/# value"),
        database_url=None,
    )
    assert "secret%3A%40%2F%23%20value" in settings.sqlalchemy_url
    assert "secret:@/# value" not in repr(settings)
