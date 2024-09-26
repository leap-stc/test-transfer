"""
Test transfer
"""

import apache_beam as beam

# from leap_data_management_utils.data_management_transforms import (
#     get_catalog_store_urls,
# )
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
import subprocess
import os

from dataclasses import dataclass

import logging
# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
# catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


src_path = "gs://leap-scratch/norlandrhagen/air_temp.zarr"
dst_path = "s3://m2lines-test/test-transfer-beam/air_temp.zarr/"


src_pattern = pattern_from_file_sequence([src_path], concat_dim="time")


logger = logging.getLogger(__name__)


@dataclass
class Transfer(beam.PTransform):
    target_store: str

    def transfer(self, source_store) -> str:
        source_store = source_store[1]

        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceClient()
        osn_id = client.access_secret_version(
            name="projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY/versions/latest"
        ).payload.data.decode("UTF-8")
        osn_secret = client.access_secret_version(
            name="projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY_SECRET/versions/latest"
        ).payload.data.decode("UTF-8")

        gcs_remote = ":gcs,env_auth=true:"
        osn_remote = f":s3,provider=Ceph,endpoint='https://nyu1.osn.mghpcc.org',access_key_id={osn_id},secret_access_key={osn_secret}:"

        ## Todo: parse this from input
        store_name = "/data-library"
        source_bucket = f"{gcs_remote}leap-persistent"
        target_bucket = f"{osn_remote}m2lines-test"
        # source_prefix = "data-library/feedstocks/GODAS"
        source_prefix = ""
        # target_prefix = "test-transfer-beam-clean-2workers"
        target_prefix = "test-transfer-500GB"

        # construct full valid rclone 'paths'
        source = os.path.join(source_bucket, source_prefix, store_name).rstrip('/')
        target = os.path.join(target_bucket, target_prefix, store_name).rstrip('/')
  
        copy_proc = subprocess.run(
            f'rclone copy -vv -P "{source}/" "{target}/"',
            shell=True,
            capture_output=False, # will expose secrets if true
            text=True, 
        )
        copy_proc.check_returncode()
        del copy_proc

        del client
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
