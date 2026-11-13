class StreamReady:
    """Non-consuming predicate for a module-owned Stream Subscriber.

    This uses only the public has_frame API. It cannot detect StateSpec; do not
    use it for state updates. Readiness does not reserve input or acquire leases.
    """
    def __init__(self, subscriber):
        self.subscriber = subscriber

    def __call__(self):
        return self.subscriber.has_frame()
