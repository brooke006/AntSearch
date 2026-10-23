"""Scheduler tests use fake workers; no network or Redis is needed."""

from dataclasses import dataclass, field
import unittest

from antsearch.scheduler import RoundRobinScheduler, Scheduler


@dataclass
class FakeWorker:
    worker_id: str
    tasks: list[str] = field(default_factory=list)


class TestRoundRobinScheduler(unittest.TestCase):
    def test_interface_cannot_be_used_without_implementation(self):
        with self.assertRaises(TypeError):
            Scheduler()

    def test_tasks_are_shared_in_turn_and_wrap_around(self):
        workers = [FakeWorker("w1"), FakeWorker("w2"), FakeWorker("w3")]
        scheduler = RoundRobinScheduler()
        for number in range(7):
            chosen = scheduler.assign([worker.worker_id for worker in workers])
            worker = next(worker for worker in workers if worker.worker_id == chosen)
            worker.tasks.append(f"t{number + 1}")
        self.assertEqual(workers[0].tasks, ["t1", "t4", "t7"])
        self.assertEqual(workers[1].tasks, ["t2", "t5"])
        self.assertEqual(workers[2].tasks, ["t3", "t6"])

    def test_empty_list_returns_none_and_preserves_turn(self):
        scheduler = RoundRobinScheduler()
        self.assertIsNone(scheduler.assign([]))
        self.assertEqual(scheduler.assign(["w1", "w2"]), "w1")
        self.assertIsNone(scheduler.assign([]))
        self.assertEqual(scheduler.assign(["w1", "w2"]), "w2")

    def test_one_worker_gets_each_turn(self):
        scheduler = RoundRobinScheduler()
        self.assertEqual([scheduler.assign(["w1"]) for _ in range(3)], ["w1"] * 3)

    def test_removed_worker_is_never_selected(self):
        scheduler = RoundRobinScheduler()
        self.assertEqual(scheduler.assign(["w1", "w2", "w3"]), "w1")
        self.assertEqual(scheduler.assign(["w2", "w3"]), "w2")
        self.assertEqual(scheduler.assign(["w2", "w3"]), "w3")

    def test_new_worker_gets_a_turn(self):
        scheduler = RoundRobinScheduler()
        self.assertEqual(scheduler.assign(["w1"]), "w1")
        self.assertEqual(scheduler.assign(["w1", "w2"]), "w2")

    def test_duplicate_ids_do_not_give_extra_turns(self):
        scheduler = RoundRobinScheduler()
        ids = ["w1", "w1", "w2"]
        self.assertEqual([scheduler.assign(ids) for _ in range(4)], ["w1", "w2", "w1", "w2"])

    def test_result_callback_does_not_change_round_robin_order(self):
        scheduler = RoundRobinScheduler()
        self.assertEqual(scheduler.assign(["w1", "w2"]), "w1")
        scheduler.on_result("w1", {"period": 0.94})
        self.assertEqual(scheduler.assign(["w1", "w2"]), "w2")


if __name__ == "__main__":
    unittest.main()
