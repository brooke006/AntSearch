class Scheduler:
    def assign(self, workers):
        raise NotImplementedError
    def on_result(self, worker, result):
        raise NotImplementedError


class RoundRobinScheduler(Scheduler):
    def __init__(self):
        self.last_worker = None

    def assign(self, workers):
        available_workers = []
        for worker in workers:
            if worker not in available_workers:
                available_workers.append(worker)

        if len(available_workers) == 0:
            return None

        position = 0
        if self.last_worker in available_workers:
            position = available_workers.index(self.last_worker)
            position = position + 1

        if position == len(available_workers):
            position = 0
        worker = available_workers[position]
        self.last_worker = worker
        return worker

    def on_result(self, worker, result):
        pass
