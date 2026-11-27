import math

import pytest

from antsearch.validation import Decision, answer, decide, groups, matches

RESULT = {"star_id": "TOI-102", "sector": 1, "period": 4.4119, "t0": 1325.5, "duration": 0.12, "depth": 0.0011, "power": 31.2}


def test_timings_are_ignored():
    assert matches({**RESULT, "download_seconds": 3.0, "compute_seconds": 9.1}, {**RESULT, "compute_seconds": 2.2})


def test_tiny_rounding_differences_are_allowed():
    assert matches(RESULT, {**RESULT, "depth": RESULT["depth"] * (1 + 1e-9)})


def test_a_mac_and_a_linux_container_agree_on_the_same_star():
    # TOI-102 from the same cached file, on macOS (numpy with Accelerate) and in the Docker image.
    mac = {"star_id": "TOI-102", "sector": 1, "period": 4.413048825974573, "t0": 1326.07557805538, "duration": 0.12,
           "depth": 0.013100431396754366, "power": 0.2829348858266935, "snr": 317.0315950383398}
    linux = {**mac, "depth": 0.013100398384742126, "power": 0.2829341728539958, "snr": 317.0790039945993}

    assert matches(mac, linux)
    assert not matches(mac, linux, tolerance=1e-6)  # the old tolerance sent every such star to a third run


@pytest.mark.parametrize(
    "other",
    [
        {**RESULT, "period": 2.2059},
        {**RESULT, "depth": RESULT["depth"] * 1.05},
        {**RESULT, "sector": 2},
        {**RESULT, "star_id": "TOI-103"},
        {key: value for key, value in RESULT.items() if key != "power"},
        {**RESULT, "extra": 1},
        {**RESULT, "sector": "1"},
        {"error": "NoDataError", "message": "no light curve"},
    ],
    ids=["period", "depth", "sector", "star", "missing key", "extra key", "type", "error"],
)
def test_a_different_answer_does_not_match(other):
    assert not matches(RESULT, other)


def test_failures_match_on_their_error_type_only():
    assert matches({"error": "NoDataError", "message": "worker-1: none"}, {"error": "NoDataError", "message": "worker-2: none"})
    assert not matches({"error": "NoDataError"}, {"error": "DownloadError"})
    assert answer({"error": "NoDataError", "message": "x", "compute_seconds": 1}) == {"error": "NoDataError"}


def test_nested_values_and_special_numbers():
    assert matches({"a": [1.0, {"b": 2.0}]}, {"a": [1.0, {"b": 2.0 + 1e-12}]})
    assert not matches({"a": [1.0, 2.0]}, {"a": [1.0]})
    assert matches({"x": math.nan}, {"x": math.nan})
    assert not matches({"x": True}, {"x": 1})
    assert matches({"x": 0.0}, {"x": 1e-15})


def test_groups_collect_matching_results_in_order():
    a, b = RESULT, {**RESULT, "period": 9.9}
    assert groups([a, b, {**a, "compute_seconds": 1}, b, {"error": "X"}]) == [[0, 2], [1, 3], [4]]


@pytest.mark.parametrize(
    "results, outstanding, expected",
    [
        ([RESULT], 1, Decision("waiting")),
        ([RESULT, RESULT], 0, Decision("accepted", agreed=(0, 1))),
        ([RESULT, {**RESULT, "period": 1.0}], 0, Decision("rerun", runs=1)),
        ([RESULT, {**RESULT, "period": 1.0}], 1, Decision("waiting")),
        ([RESULT, {**RESULT, "period": 1.0}, RESULT], 0, Decision("accepted", agreed=(0, 2))),
        ([{**RESULT, "period": 1.0}, RESULT, RESULT], 0, Decision("accepted", agreed=(1, 2))),
        ([RESULT, {**RESULT, "period": 1.0}, {**RESULT, "period": 2.0}], 0, Decision("unresolved")),
    ],
    ids=["first", "match", "mismatch", "mismatch, rerun already queued", "third agrees", "third sides with second", "no two agree"],
)
def test_two_replicas(results, outstanding, expected):
    assert decide(results, outstanding, quorum=2, max_runs=3) == expected


def test_one_replica_accepts_the_first_result():
    assert decide([{"error": "boom"}], 0, quorum=1, max_runs=1) == Decision("accepted", agreed=(0,))


def test_three_replicas_get_up_to_five_runs():
    a, b, c = RESULT, {**RESULT, "period": 1.0}, {**RESULT, "period": 2.0}
    assert decide([a, b, c], 0, quorum=3, max_runs=5) == Decision("rerun", runs=2)
    assert decide([a, b, c, a], 1, quorum=3, max_runs=5) == Decision("waiting")
    assert decide([a, b, c, a, a], 0, quorum=3, max_runs=5) == Decision("accepted", agreed=(0, 3, 4))
    assert decide([a, b, c, a, b], 0, quorum=3, max_runs=5) == Decision("unresolved")
