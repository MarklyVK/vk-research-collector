"""Ошибки VK без включения секретных параметров запроса."""


class VKError(Exception):
    """Базовая безопасная ошибка VK."""


class VKAPIError(VKError):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"VK API вернул ошибку {code}: {message}")


class VKTokensUnavailable(VKError):
    """Все токены отключены; работу нужно поставить на паузу."""


class VKRetryExhausted(VKError):
    """Повторяемая операция исчерпала интервалы повторов."""


class VKMethodUnavailable(VKError):
    """Точный VK method временно заблокирован у всех доступных токенов."""

    def __init__(self, method: str, retry_at: float | None, error_code: int | None) -> None:
        self.method = method
        self.retry_at = retry_at
        self.error_code = error_code
        suffix = f" до {retry_at:.3f}" if retry_at is not None else ""
        super().__init__(f"Метод VK {method} временно недоступен{suffix}")
