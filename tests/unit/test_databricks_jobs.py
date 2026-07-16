import unittest

from include.databricks.jobs import (
    BRONZE_JOB_ID,
    BRONZE_JOB_NAME,
    get_bronze_job_id,
    get_databricks_job_registry,
    get_job_id,
)


class DatabricksJobRegistryTests(unittest.TestCase):
    def test_returns_bronze_job_id(self):
        self.assertEqual(get_bronze_job_id(), BRONZE_JOB_ID)

    def test_returns_job_id_for_registered_keys(self):
        self.assertEqual(get_job_id("bronze"), BRONZE_JOB_ID)
        self.assertEqual(get_job_id(BRONZE_JOB_NAME), BRONZE_JOB_ID)

    def test_registry_is_lightweight_and_copyable(self):
        registry = get_databricks_job_registry()
        self.assertEqual(registry["bronze"], BRONZE_JOB_ID)
        self.assertEqual(registry[BRONZE_JOB_NAME.casefold()], BRONZE_JOB_ID)

    def test_unknown_job_key_raises_value_error(self):
        with self.assertRaises(ValueError):
            get_job_id("silver")


if __name__ == "__main__":
    unittest.main()
