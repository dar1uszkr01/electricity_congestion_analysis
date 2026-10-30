"""Run with python -m unittest discover -s ingestion/nve -p 'test_*.py'."""
import unittest
from datetime import date
from nve_snow_hydrology import date_windows, normalize, select_themes, validate_locations


class SnowHydrologyTests(unittest.TestCase):
    def setUp(self):
        self.location = {"location_id": "example", "x": 130000, "y": 6800000}
        self.payload = dict(Theme="swe", FullName="Snow water equivalent", X=130000,
                            Y=6800000, StartDate="28.02.2024 06:00:00",
                            EndDate="01.03.2024 06:00:00", TimeResolution=1440,
                            NoDataValue=65535, Unit="mm", Altitude=1226,
                            Data=[0, 65535, 134.7])

    def test_windows_cover_leap_day_without_overlap(self):
        self.assertEqual(list(date_windows("2024-02-28", "2024-03-02", 2)),
                         [("2024-02-28", "2024-02-29"), ("2024-03-01", "2024-03-02")])
        with self.assertRaises(ValueError):
            list(date_windows("2025-01-02", "2025-01-01"))

    def test_source_times_and_missing_values(self):
        rows = normalize(self.payload, self.location, "swe", "2024-02-28", "2024-03-01")
        self.assertEqual(rows[0]["value"], 0.0)
        self.assertFalse(rows[0]["is_missing"])
        self.assertIsNone(rows[1]["value"])
        self.assertEqual(rows[1]["observation_date"], date(2024, 2, 29))
        self.assertEqual(rows[2]["source_time"], "2024-03-01T06:00:00")
        self.payload.update(Theme="qtt", NoDataValue=255, Data=[255, None, 0])
        rows = normalize(self.payload, self.location, "qtt", "2024-02-28", "2024-03-01")
        self.assertEqual([r["is_missing"] for r in rows], [True, True, False])

    def test_reject_incomplete_or_wrong_response(self):
        for changes in ({"Data": [1]}, {"Theme": "tm"}, {"X": 1},
                        {"TimeResolution": 180}, {"EndDate": "02.03.2024 06:00:00"},
                        {"Data": [1, float("nan"), 2]}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize(dict(self.payload, **changes), self.location,
                          "swe", "2024-02-28", "2024-03-01")

    def test_locations_and_theme_availability(self):
        validate_locations([self.location])
        for locations in ([], [self.location, self.location],
                          [dict(self.location, x=7.2)]):
            with self.assertRaises(ValueError):
                validate_locations(locations)
        metadata = [dict(Name="swe", TimeResolutionInMinutes=1440,
                         FirstDateInTimeSerie="1957-01-01")]
        self.assertIn("swe", select_themes(metadata, ["swe"], "2020-01-01"))
        for themes, start in ((["unknown"], "2020-01-01"), (["swe"], "1950-01-01")):
            with self.assertRaises(ValueError):
                select_themes(metadata, themes, start)


if __name__ == "__main__":
    unittest.main()
