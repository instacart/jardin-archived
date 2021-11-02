

class BaseSubscriber:
    def report_event(self, event_name, start_time, end_time, duration, tags={}):
        raise NotImplementedError
