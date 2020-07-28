# Input Data

Input files are located in `data/{project}/`. For projects with CSV-based data, each file
`{name}.csv` (or set of files `{name}_{number}.csv`) is paired with a manually maintained
`{name}_0.csv` which contains the header line defining the column names. When/if the input
data changes the columns, this file must be updated to match the input data.

For the Provenance projects, data is uploaded from GRI to S3 in a dated subfolder of
`/jpgt-or-provenance-01/provenance_batch/data/stardata/exports/`. After verifying that
columns still match the header file (described above), these files can be moved to the
location of production files in S3 (`/jpgt-or-provenance-01/provenance_batch/data/{project}/`).
For the sales project, the [`promote_sales_data_to_production.sh`](../scripts/promote_sales_data_to_production.sh)
script simplifies this task.

# Environment Variables

When running the pipeline docker container, the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables must be set to allow data to be copied to/from S3.

