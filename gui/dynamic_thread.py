import threading
import queue

class DynamicThread(threading.Thread):
    def __init__(self, func, args=None, kwargs=None):
        super(DynamicThread, self).__init__()
        self.func = func
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.queue = queue.Queue()

    def run(self):
        
        try:
            result = self.func(*self.args, **self.kwargs)
            self.queue.put(result)
        except Exception as e:
            self.queue.put(e)

    def get_result(self):
        return self.queue.get()

    def is_alive(self):
        return super().is_alive()