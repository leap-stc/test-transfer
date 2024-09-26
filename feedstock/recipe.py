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
        
        # osn_config_proc = subprocess.run(
        #     rclone_create_config_osn_str,
        #     shell=True,
        #     capture_output=True,
        #     text=True,
        # )
        # logger.warning(osn_config_proc)
        # osn_config_proc.check_returncode()



        gcs_remote = ':"google cloud storage",env_auth=true:'
        # # this does not work due to the colon in the endpoint? Gahhh this is awful...
        osn_remote = f":s3,provider=Ceph,endpoint='https://nyu1.osn.mghpcc.org',access_key_id={osn_id},secret_access_key={osn_secret}:"
        
        list_gcs = subprocess.run(
            f'rclone -vv lsf "{gcs_remote}leap-scratch/"',
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warning(list_gcs)


        list_osn = subprocess.run(
            f'rclone -vv lsf "{osn_remote}m2lines-test/"',
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.warning(list_osn)
        list_osn.check_returncode()


        copy_proc = subprocess.run(
            f'rclone copy -vv -P {gcs_remote}leap-persistent/data-library/feedstocks/GODAS/GODAS_surface_level.zarr/ {osn_remote}test-transfer-beam/GODAS_surface_level.zarr',
            shell=True, #consider false
            capture_output=True, #set to false once we have this working!
            text=True,
        )
        copy_proc.check_returncode()
        logger.warning(copy_proc)

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
