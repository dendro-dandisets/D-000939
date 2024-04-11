import dandi.dandiarchive as da
from src.Pipeline import Pipeline, PipelineJob, PipelineJobInput, PipelineJobOutput, PipelineJobRequiredResources, PipelineImportedFile
from src.utils import _remote_file_exists


def main():
    #  Large-scale recordings of head-direction cells in mouse postsubiculum
    dandiset_id = '000939'
    dandiset_version = '0.240327.2229'  # march 27, 2024
    dendro_project_id = 'd02200fd'  # https://dendro.vercel.app/project/d02200fd?tab=project-home
    # project: D-000946
    pipeline = Pipeline(project_id=dendro_project_id)

    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

        num_consecutive_not_nwb = 0
        num_consecutive_not_found = 0
        num_assets_processed = 0
        for asset_obj in dandiset.get_assets('path'):
            if not asset_obj.path.endswith(".nwb"):
                num_consecutive_not_nwb += 1
                if num_consecutive_not_nwb >= 20:
                    # For example, this is important for 000026 because there are so many non-nwb assets
                    print("Stopping dandiset because too many consecutive non-NWB files.")
                    break
                continue
            else:
                num_consecutive_not_nwb = 0
            if num_consecutive_not_found >= 20:
                print("Stopping dandiset because too many consecutive missing files.")
                break
            if num_assets_processed >= 100:
                print("Stopping dandiset because 100 assets have been processed.")
                break
            asset_id = asset_obj.identifier
            asset_path = asset_obj.path
            lindi_url = f'https://lindi.neurosift.org/dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
            if not _remote_file_exists(lindi_url):
                num_consecutive_not_found += 1
                continue
            print(f'Processing file {asset_path} ({num_assets_processed})')
            name = f'{dandiset_id}/{asset_path}.lindi.json'
            pipeline.add_imported_file(PipelineImportedFile(
                fname=f'imported/{name}',
                url=lindi_url,
                metadata={
                    'dandisetId': dandiset_id,
                    'dandisetVersion': dandiset_version,
                    'dandiAssetId': asset_id
                }
            ))
            create_autocorrelograms(
                pipeline=pipeline,
                input=f'imported/{name}',
                output=f'generated/{name}/autocorrelograms.nwb.lindi.json',
                metadata={
                    'dandisetId': dandiset_id,
                    'dandisetVersion': dandiset_version,
                    'dandiAssetId': asset_id,
                    'supplemental': True
                }
            )

            num_assets_processed += 1
    print('Submitting pipeline')
    pipeline.submit()


def create_autocorrelograms(*, pipeline: Pipeline, input: str, output: str, metadata: dict):
    pipeline.add_job(PipelineJob(
        processor_name='neurosift-1.autocorrelograms',
        inputs=[
            PipelineJobInput(name='input', fname=input)
        ],
        outputs=[
            PipelineJobOutput(name='output', fname=output, metadata=metadata)
        ],
        parameters=[],
        required_resources=PipelineJobRequiredResources(
            num_cpus=4,
            num_gpus=0,
            memory_gb=16,
            time_sec=60 * 60 * 24
        ),
        run_method='local'
    ))


if __name__ == '__main__':
    main()
