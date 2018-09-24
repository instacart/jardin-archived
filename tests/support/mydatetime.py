import datetime

class _mydatetime(datetime.datetime):
    @property
    def nanosecond(self):
        return 0
