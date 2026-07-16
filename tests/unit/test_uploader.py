import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from include.framework.spark import uploader


class FakeMinio:
    def __init__(self, *args, **kwargs):
        self.uploaded = []
        self.buckets = set()

    def bucket_exists(self, bucket_name):
        return bucket_name in self.buckets

    def make_bucket(self, bucket_name):
        self.buckets.add(bucket_name)

    def fput_object(self, bucket_name, object_name, file_path):
        self.uploaded.append((bucket_name, object_name, file_path))


class UploadDirectoryToMinioTests(unittest.TestCase):
    def test_uploads_nested_files_with_relative_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir) / "tmp" / "bronze" / "sales"
            nested_dir = directory / "nested"
            nested_dir.mkdir(parents=True)

            (directory / "orders.csv").write_text("id\n1\n", encoding="utf-8")
            (nested_dir / "part-0000.csv").write_text("id\n2\n", encoding="utf-8")

            fake_client = FakeMinio()
            with patch.dict(
                os.environ,
                {
                    "MINIO_ENDPOINT": "localhost:9000",
                    "MINIO_ACCESS_KEY": "access",
                    "MINIO_SECRET_KEY": "secret",
                    "MINIO_SECURE": "False",
                },
                clear=False,
            ):
                with patch.object(uploader, "Minio", return_value=fake_client):
                    result = uploader.upload_directory_to_minio(str(directory), "test-bucket")

            self.assertEqual(result, "bronze/sales")
            uploaded_objects = [object_name for _, object_name, _ in fake_client.uploaded]
            self.assertEqual(uploaded_objects.count("bronze/sales/orders.csv"), 1)
            self.assertEqual(uploaded_objects.count("bronze/sales/nested/part-0000.csv"), 1)
            self.assertEqual(sorted(uploaded_objects), [
                "bronze/sales/nested/part-0000.csv",
                "bronze/sales/orders.csv",
            ])


if __name__ == "__main__":
    unittest.main()
