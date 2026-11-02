from types import SimpleNamespace

class Tensor:
    def __init__(self, values):
        self.values = values
    def cpu(self):
        return self
    def tolist(self):
        return self.values

class Boxes:
    xyxy = Tensor([[10, 20, 100, 200]])
    cls = Tensor([1])
    conf = Tensor([.9])
    id = Tensor([42])
    def __len__(self):
        return 1

class FakeModel:
    names = {0: 'pessoa_normal', 1: 'pessoa_queda'}
    def track(self, *a, **kw):
        return [SimpleNamespace(boxes=Boxes())]

class FakeModelMulti:
    def __init__(self, sequence_classes=None):
        self.names = {0: 'pessoa_desequilibrio', 1: 'pessoa_normal', 2: 'pessoa_queda'}
        self.sequence_classes = sequence_classes or [0]
        self.call_count = 0
        self.callbacks = {'on_predict_postprocess_end': []}

    def add_callback(self, event, callback):
        self.callbacks[event].append(callback)

    def track(self, *a, **kw):
        idx = min(self.call_count, len(self.sequence_classes) - 1)
        cls_id = self.sequence_classes[idx]
        self.call_count += 1
        class DynamicBoxes:
            xyxy = Tensor([[10, 20, 100, 200]])
            cls = Tensor([cls_id])
            conf = Tensor([0.88])
            id = Tensor([42])
            def __len__(self):
                return 1
        boxes = DynamicBoxes()
        boxes.id = None
        predictor = SimpleNamespace(results=[SimpleNamespace(boxes=boxes)])
        for callback in self.callbacks['on_predict_postprocess_end']:
            callback(predictor)
        boxes.id = Tensor([42])
        return predictor.results

class FakeCapture:
    def __init__(self):
        self.left = 5
        self.closed = False
    def isOpened(self):
        return True
    def get(self, prop):
        return 1000
    def read(self):
        self.left -= 1
        return (True, object()) if self.left >= 0 else (False, None)
    def release(self):
        self.closed = True

class FakePublisher:
    def __init__(self, cfg, outbox, stopping=None):
        self.hb = []
        self.stop_event = SimpleNamespace(set=lambda: None)
        self.fatal_error = None
    def start(self):
        pass
    def heartbeat(self, *a):
        raise AssertionError('Vídeo não pode emitir heartbeat')
    def is_alive(self):
        return False
    def join(self, **kw):
        pass
