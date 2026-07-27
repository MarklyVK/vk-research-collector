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
