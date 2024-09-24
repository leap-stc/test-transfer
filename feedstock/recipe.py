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

        # from google.cloud import secretmanager
        # client = secretmanager.SecretManagerServiceClient()
        # aws_id = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY/versions/latest").payload.data.decode("UTF-8")
        # aws_secret = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY_SECRET/versions/latest").payload.data.decode("UTF-8")

        # ToDo: How do we get service_account_credentials ie gcs_credentials
        # os.environ['RCLONE_CONFIG'] = f"""
        #     [ceph]
        #     type = s3
        #     env_auth = false
        #     access_key_id = {aws_id}
        #     secret_access_key = {aws_secret}
        #     endpoint = https://nyu1.osn.mghpcc.org
        #     """

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

transfer = beam.Create(src_pattern.items()) | beam.Map(
    print
)  # | Transfer(target_store=dst_path)
