class APIException(Exception):
    """Базовое исключение API"""
    pass

class MigrationException(APIException):
    """Исключение при миграции"""
    pass

class ClassificationException(APIException):
    """Исключение при классификации"""
    pass
