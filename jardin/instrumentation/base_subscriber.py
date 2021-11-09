from jardin.instrumentation.event import Event

class BaseSubscriber:
    def report_event(self, event: Event):
        raise NotImplementedError
