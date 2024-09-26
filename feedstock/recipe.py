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


        # rclone_create_config_osn_str = f"rclone config create osn s3 \
        #       provider=Ceph endpoint=https://nyu1.osn.mghpcc.org \
        #         --access_key_id={osn_id} \
        #         --secret_access_key={osn_secret}"
        



        # Todo!
        # 1. use old method for osn create with subprocess
        # 2. rclone config file -> gives path to config file
        # 3. use rclone config file path to write gcs
        #
        rclone_create_config_file_str = f"""
        [gcs]
        type = google cloud storage
        env_auth = true

        [osn-test]
        type = s3
        provider = Ceph
        endpoint = https://nyu1.osn.mghpcc.org
        access_key_id = {osn_id}
        secret_access_key = {osn_secret}
        no_touch_bucket = true

        """
            
        with open(".config/rclone/rclone.conf", "w+") as of:
            of.write(rclone_create_config_file_str)

        
        list_configs = subprocess.run(
            [
                "rclone config file",
            ],
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warning(list_configs)

        list_remotes = subprocess.run(
            [
                "rclone listremotes",
            ],
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warning(list_remotes)

        ls_out_osn = subprocess.run(
            "rclone ls osn:m2lines-test",
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warn(ls_out_osn)

        ls_out_gcp = subprocess.run(
            "rclone ls gcs:leap-scratch/norlandrhagen/",
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warn(ls_out_gcp)

        # copy_out = subprocess.run(
        #     f"rclone -v copy gcs:leap-scratch/norlandrhagen/air_temp.nc osn:{bucket_name}air_temp_rclone.zarr/",
        #     shell=True,
        #     capture_output=True,
        #     text=True,
        # )
        # logger.warn(copy_out)
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
