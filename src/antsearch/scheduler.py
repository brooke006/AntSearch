class Scheduler:
    def assign(self, worker_id, tasks, workers):
        raise NotImplementedError

    def on_result(self, worker_id, result):
        raise NotImplementedError


class RoundRobinScheduler(Scheduler):
    def __init__(self):
        self.last_worker = None
        self.previous_workers = []

    def assign(self, worker_id, tasks, workers):
        available_workers = []
        for worker in workers:
            if worker not in available_workers:
                available_workers.append(worker)

        if len(tasks) == 0 or len(available_workers) == 0:
            return None

        if worker_id not in available_workers:
            return None

        next_worker = available_workers[0]

        if self.last_worker in available_workers:
            position = available_workers.index(self.last_worker) + 1
            if position == len(available_workers):
                position = 0
            next_worker = available_workers[position]

        elif self.last_worker in self.previous_workers:
            # Continue after the removed worker using the previous order.
            position = self.previous_workers.index(self.last_worker) + 1
            worker_order = self.previous_workers[position:] + self.previous_workers[:position]
            for worker in worker_order:
                if worker in available_workers:
                    next_worker = worker
                    break

        if worker_id != next_worker:
            return None

        self.last_worker = worker_id
        self.previous_workers = available_workers[:]
        return tasks[0]

    def on_result(self, worker_id, result):
        pass
