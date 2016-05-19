"Core exceptions raised by the hive executor"


class HiveError(Exception):
    pass

class SystemCommandExecuteError(Exception):
    pass

class HiveUnfoundError(HiveError):
    pass


class HiveCommandExecuteError(HiveError):
    pass


class ConnectionError(HiveError):
    pass


class TimeoutError(HiveError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(HiveError):
    pass


class ResponseError(HiveError):
    pass


class DataError(HiveError):
    pass


class PubSubError(HiveError):
    pass


class WatchError(HiveError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass
