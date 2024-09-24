"""
Test transfer
"""

import apache_beam as beam

# from leap_data_management_utils.data_management_transforms import (
#     get_catalog_store_urls,
# )
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
import subprocess


from dataclasses import dataclass

import logging
# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
# catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


src_path = "gs://leap-scratch/norlandrhagen/air_temp.zarr"
dst_path = "s3://m2lines-test/test-transfer/air_temp.zarr/"


src_pattern = pattern_from_file_sequence([src_path], concat_dim="time")


logger = logging.getLogger(__name__)


@dataclass
class Transfer(beam.PTransform):
    target_store: str

    def transfer(self, source_store) -> str:
        source_store = source_store[1]
        ls_out = subprocess.run(
            "rclone --help", shell=True, capture_output=True, text=True
        )
        logger.warn(ls_out)

        # ToDo: Fix input store path from gcs to s3.
        # from google.cloud import secretmanager
        # client = secretmanager.SecretManagerServiceClient()
        # aws_id = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY/versions/latest").payload.data.decode("UTF-8")
        # aws_secret = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY_SECRET/versions/latest").payload.data.decode("UTF-8")
        # os.environ["AWS_ACCESS_KEY_ID"] = aws_id
        # os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
        # command = f"s5cmd --endpoint-url https://nyu1.osn.mghpcc.org cp {source_store} {self.target_store}"
        # logger.warn(command)

        # ls_out = subprocess.run(command, shell=True, capture_output=True, text=True)
        # logger.warn(ls_out)
        # del client
        return self.target_store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self.transfer)


# with beam.Pipeline() as p:
#     (
#     p
#     | beam.Create(src_pattern.items())
#     | Transfer(target_store = dst_path)
#     | beam.Map(print)

#     )
# recipe = (
#     beam.Create(pattern.items())

transfer = beam.Create(src_pattern.items()) | Transfer(target_store=dst_path)
