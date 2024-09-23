"""
Test transfer
"""

import os
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
)
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")



src_path = ['gs://leap/<DATASET.zarr>']
dst_path =  's3://m2lines-test/<OSN-LEAP-BUCKET-NAME/ds-name.zarr'

import subprocess
src_pattern = pattern_from_file_sequence(src_path[0], concat_dim="time")



class S5cmdTransfer(beam.DoFn):
    def transfer(self, src_path, dst_path):

        # TODO: Add keys

        command = f"s5cmd --profile <ADD PROFILE> --endpoint-url https://nyu1.osn.mghpcc.org cp {src_path} {dst_path}'"
        subprocess.run(command, shell=True, capture_output=True, text=True)

        return dst_path

transfer = (
    beam.Create(src_pattern.items())
    | beam.ParDo()
    | beam.ParDo(S5cmdTransfer(), src_path, dst_path)

)

