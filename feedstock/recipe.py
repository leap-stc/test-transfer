"""
Test transfer
"""

import os
import apache_beam as beam
# from leap_data_management_utils.data_management_transforms import (
#     get_catalog_store_urls,
# )
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
# catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


src_path = 'gs://leap-scratch/norlandrhagen/air_temp.zarr'
dst_path =  's3://m2lines-test/test-transfer/air_temp.zarr/*'

import subprocess
src_pattern = pattern_from_file_sequence([src_path], concat_dim="time")


import xarray as xr 
from dataclasses import dataclass

import logging 

logger = logging.getLogger(__name__)
@dataclass
class Transfer(beam.PTransform):
    target_store: str

    def transfer(self, source_store) -> str:
        import os
        logger.debug(f'transfer from {source_store[1]} to {self.target_store}')
        source_store = source_store[1]

        # ToDo: Fix input store path from gcs to s3.
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        aws_id = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY/versions/latest").payload.data.decode("UTF-8")
        aws_secret = client.access_secret_version(name=f"projects/leap-pangeo/secrets/OSN_CATALOG_BUCKET_KEY_SECRET/versions/latest").payload.data.decode("UTF-8")
        os.environ["aws_access_key_id"] = aws_id
        os.environ["aws_secret_access_key"] = aws_secret

        command = f"s5cmd --endpoint-url https://nyu1.osn.mghpcc.org {source_store} '{self.target_store}'"
        logger.warn(command)

        ls_out = subprocess.run(command, shell=True, capture_output=True, text=True)
        logger.warn(ls_out)
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
    
transfer = (
    beam.Create(src_pattern.items())    
    | Transfer(target_store = dst_path)
)

