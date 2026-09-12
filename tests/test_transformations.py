import unittest
from datetime import time, timedelta

from dags.api.datawarehouse.data_transformations import parse_duration, transform_data


class ParseDurationTests(unittest.TestCase):
    def test_parses_seconds_minutes_hours_and_days(self):
        self.assertEqual(parse_duration("PT42S"), timedelta(seconds=42))
        self.assertEqual(parse_duration("PT12M34S"), timedelta(minutes=12, seconds=34))
        self.assertEqual(parse_duration("PT1H2M3S"), timedelta(hours=1, minutes=2, seconds=3))
        self.assertEqual(parse_duration("P1DT2H"), timedelta(days=1, hours=2))

    def test_rejects_invalid_duration(self):
        with self.assertRaises(ValueError):
            parse_duration("12:34")


class TransformDataTests(unittest.TestCase):
    def test_classifies_short_without_mutating_input(self):
        row = {"Video_ID": "sample00002", "Duration": "PT60S"}
        transformed = transform_data(row)

        self.assertEqual(transformed["Duration"], time(0, 1))
        self.assertEqual(transformed["Video_Type"], "short")
        self.assertEqual(row["Duration"], "PT60S")

    def test_classifies_long_form_video(self):
        transformed = transform_data({"Video_ID": "sample00001", "Duration": "PT61S"})
        self.assertEqual(transformed["Video_Type"], "normal")

    def test_rejects_duration_that_postgres_time_cannot_represent(self):
        with self.assertRaises(ValueError):
            transform_data({"Video_ID": "sample00003", "Duration": "P1D"})


if __name__ == "__main__":
    unittest.main()
