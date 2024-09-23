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
dst_path =  's3://m2lines-test/test-transfer/air_temp.zarr'

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
        
        logger.debug(f'transfer from {source_store} to {self.target_store}')
        print(self.target_store)
        
        # ToDo: Add profile/config from env vars for OSN?
        # ToDo: Fix input store path from gcs to s3.
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        client.get_secret(request={"name": "DUMMY"})
        logger.debug(client.get_secret(request={"name": "DUMMY"}))

        # command = f"s5cmd --profile <ADD PROFILE> --endpoint-url https://nyu1.osn.mghpcc.org cp {source_store} {self.target_store}'"
        # subprocess.run(command, shell=True, capture_output=True, text=True)

        return self.target_store
    

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self.transfer)



# with beam.Pipeline() as p:
#     (
#     p
#     | beam.Create([src_pattern])
#     | Transfer(target_store = dst_path)
#     | beam.Map(print)

#     )      
# recipe = (
#     beam.Create(pattern.items())
    
transfer = (
    beam.Create(src_pattern.items())    
    | Transfer(target_store = dst_path)
)

