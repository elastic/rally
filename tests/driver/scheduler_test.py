import random
from unittest import TestCase

from esrally import exceptions
from esrally.driver import scheduler


class WaitTimeTests(TestCase):
    def test_either_target_throughput_or_target_interval(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            scheduler._calculate_wait_time({
                "target-throughput": 10,
                "target-interval": 3
            })
        self.assertEqual("Found target-interval [3] and target-throughput [10] but only one of them is allowed.", ctx.exception.args[0])

    def test_calculate_wait_time_based_on_throughput(self):
        self.assertEqual(scheduler._calculate_wait_time({
            "target-throughput": 10,
            "clients": 4
        }), 4 / 10)

    def test_calculate_wait_time_based_on_target_interval(self):
        self.assertEqual(scheduler._calculate_wait_time({
            "target-interval": 2,
            "clients": 8
        }), 16)

    def test_calculate_wait_time_zero(self):
        self.assertEqual(scheduler._calculate_wait_time({"clients": 4}), 0)
        self.assertEqual(scheduler._calculate_wait_time({}), 0)


class SchedulerTestCase(TestCase):
    ITERATIONS = 10000

    def assertRateEquals(self, sched, expected_average_rate, msg="", relative_delta=0.02):
        sum = 0
        for i in range(0, SchedulerTestCase.ITERATIONS):
            tn = sched.next(0)
            # schedule must not go backwards in time
            self.assertGreaterEqual(tn, 0, msg)
            sum += tn
        actual_average_rate = sum / SchedulerTestCase.ITERATIONS

        expected_lower_bound = (1.0 - relative_delta) * expected_average_rate
        expected_upper_bound = (1.0 + relative_delta) * expected_average_rate

        self.assertGreaterEqual(actual_average_rate, expected_lower_bound,
                                "%s: expected target rate to be >= %f but was %f." % (msg, expected_lower_bound, actual_average_rate))
        self.assertLessEqual(actual_average_rate, expected_upper_bound,
                             "%s: expected target rate to be <= %f but was %f." % (msg, expected_upper_bound, actual_average_rate))


class DeterministicSchedulerTests(SchedulerTestCase):
    def test_schedule_matches_expected_target_throughput(self):
        clients = random.randint(1, 16)
        target_throughput = random.randint(10, 1000)

        s = scheduler.DeterministicScheduler({
            "target-throughput": target_throughput,
            "clients": clients
        })
        self.assertRateEquals(s, clients / target_throughput, "%d clients with %d target throughput" % (clients, target_throughput))

    def test_schedule_unlimited(self):
        # no params -> no limit
        s = scheduler.DeterministicScheduler({})
        self.assertRateEquals(s, 0)


class PoissonSchedulerTests(SchedulerTestCase):
    def test_schedule_matches_expected_target_throughput(self):
        clients = random.randint(1, 16)
        target_throughput = random.randint(10, 1000)
        s = scheduler.PoissonScheduler({
            "target-throughput": target_throughput,
            "clients": clients
        })
        self.assertRateEquals(s, clients / target_throughput, "%d clients with %d target throughput" % (clients, target_throughput))

    def test_schedule_unlimited(self):
        # no params -> no limit
        s = scheduler.PoissonScheduler({})
        self.assertRateEquals(s, 0)
