#!/bin/bash

# Copy files from the S3 staging area to the production area.
# By default, this script will look for staging data with a timestamp from today.
# A custom date can be supplied as a command line argument to use staging data
# transferred on a different day:
#
#     ./promote_sales_data_to_production.sh 2020-07-15

set -e

TODAY=`date -u +"%Y-%m-%d"`
DATE=${1-$TODAY}

SRC="s3://jpgt-or-provenance-01/provenance_batch/data/stardata/exports/make_csv_files_${DATE}/"
DST=s3://jpgt-or-provenance-01/provenance_batch/data/sales/

aws s3 sync --exclude '*' --include 'sales_*' $SRC $DST
