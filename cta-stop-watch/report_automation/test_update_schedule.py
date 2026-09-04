"""
Tests for update_schedule. Run from this directory with:

    python -m unittest test_update_schedule
"""

import unittest
import pandas as pd
from update_schedule import build_bus_stop_time

# Tests -----------------------------------------------------------------------


class BuildBusStopTimeTests(unittest.TestCase):
    """
    arrival_time values below are taken from the CTA google_transit.zip feed
    """

    def assert_times(self, times: list[str], expected: list[str]) -> None:
        dates = pd.Series(["20260810"] * len(times))
        result = build_bus_stop_time(dates, pd.Series(times))
        pd.testing.assert_series_equal(
            result, pd.Series(pd.to_datetime(expected)), check_names=False
        )

    def test_times_within_the_service_day_keep_their_date(self):
        self.assert_times(
            ["06:24:00", "12:00:00", "23:59:30"],
            ["2026-08-10 06:24:00", "2026-08-10 12:00:00", "2026-08-10 23:59:30"],
        )

    def test_hour_24_rolls_over_and_keeps_minutes_and_seconds(self):
        self.assert_times(
            ["24:00:00", "24:06:24", "24:24:00", "24:32:24"],
            [
                "2026-08-11 00:00:00",
                "2026-08-11 00:06:24",
                "2026-08-11 00:24:00",
                "2026-08-11 00:32:24",
            ],
        )

    def test_hour_25_rolls_over_instead_of_being_dropped(self):
        self.assert_times(
            ["25:08:00", "25:14:19", "25:22:38"],
            ["2026-08-11 01:08:00", "2026-08-11 01:14:19", "2026-08-11 01:22:38"],
        )

    def test_unusable_times_become_nat(self):
        dates = pd.Series(["20260810"] * 3)
        result = build_bus_stop_time(dates, pd.Series([None, "", "not a time"]))
        self.assertTrue(result.isna().all())

    def test_result_is_a_datetime_series(self):
        result = build_bus_stop_time(pd.Series(["20260810"]), pd.Series(["24:24:00"]))
        self.assertEqual(result.dtype, "datetime64[ns]")


# End -------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
