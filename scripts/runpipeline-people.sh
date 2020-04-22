#!/bin/bash

set -e

cd /home/gwilliams/pipeline
git pull

PROJECT="people"
LIMIT=2500000
# LIMIT=200
DATETIME=`date --iso-8601=minutes`
DATE=`date --iso-8601=date`
LOGFILE="/home/gwilliams/logs/pipeline-${PROJECT}-${DATE}.log"
OUTPUTPATH=/data/output
DATANAME="${PROJECT}-${DATE}"
DATAPATH="${OUTPUTPATH}/${DATANAME}"
GITREV=`git rev-parse --short HEAD`
JSON_TARFILE="${OUTPUTPATH}/${DATANAME}-jsonld.tar.gz"
NQ_TARFILE="${OUTPUTPATH}/${DATANAME}-nquads.tar.gz"
INFOFILE="${DATAPATH}/pipeline-${PROJECT}-info.txt"
AWS_OUTPUTPATH="s3://jpgt-or-provenance-01/provenance_batch/output/${PROJECT}/"

make dockerimage
mkdir -p $DATAPATH

echo "AWS key      : ${AWS_ACCESS_KEY_ID}" | tee -a $LOGFILE
echo "Git revision : ${GITREV}"            | tee -a $LOGFILE
echo "Info file    : ${INFOFILE}"          | tee -a $LOGFILE
echo "Data path    : ${DATAPATH}"          | tee -a $LOGFILE
echo "JSON Tar file: ${JSON_TARFILE}"      | tee -a $LOGFILE
echo "NQ Tar file  : ${NQ_TARFILE}"      | tee -a $LOGFILE
echo "LIMIT        : ${LIMIT}"             | tee -a $LOGFILE

echo '' > $LOGFILE
echo "Pipeline ${GITREV}; ${DATETIME}" >> $LOGFILE
date >> $LOGFILE
echo "==================================== Starting pipeline docker container" | tee -a $LOGFILE
time docker run --env GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=/services/common --env GETTY_PIPELINE_SERVICE_FILES_PATH=/services --env GETTY_PIPELINE_INPUT=/data --env GETTY_PIPELINE_OUTPUT=/output --env GETTY_PIPELINE_TMP_PATH=/output/tmp --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -v/data/input:/data:Z -v"$DATAPATH":/output:Z -v`pwd`/data:/services:Z -it pipeline make clean "fetch${PROJECT}" $PROJECT LIMIT=$LIMIT | tee -a $LOGFILE
echo "==================================== Finished pipeline docker container" | tee -a $LOGFILE
echo '' > $LOGFILE
date >> $LOGFILE
echo "==================================== Starting N-Quads docker container" | tee -a $LOGFILE
time docker run --env GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=/services/common --env GETTY_PIPELINE_SERVICE_FILES_PATH=/services --env GETTY_PIPELINE_INPUT=/data --env GETTY_PIPELINE_OUTPUT=/output --env GETTY_PIPELINE_TMP_PATH=/output/tmp --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -v/data/input:/data:Z -v"$DATAPATH":/output:Z -v`pwd`/data:/services:Z -it pipeline make nq | tee -a $LOGFILE
echo "==================================== Finished N-Quads docker container" | tee -a $LOGFILE

echo "Pipeline ${GITREV}; ${DATETIME}" >> $INFOFILE

tar --exclude='uri_to_uuid_map.json' --exclude='json_files.txt' --exclude='*.nq' -c -C $OUTPUTPATH $DATANAME | pigz > $JSON_TARFILE
echo "Created ${JSON_TARFILE}" | tee -a $LOGFILE

tar --exclude='uri_to_uuid_map.json' --exclude='*.json' --exclude '*.gz' -c -C $OUTPUTPATH $DATANAME | pigz > $NQ_TARFILE
echo "Created ${NQ_TARFILE}"

aws s3 cp $JSON_TARFILE $AWS_OUTPUTPATH
aws s3 cp $NQ_TARFILE $AWS_OUTPUTPATH
aws s3 cp "${DATAPATH}/all.nq.gz" "${AWS_OUTPUTPATH}/${PROJECT}-${DATE}-all.nq.gz"
aws s3 cp "${DATAPATH}/meta.nq.gz" "${AWS_OUTPUTPATH}/${PROJECT}-${DATE}-meta.nq.gz"

date >> $LOGFILE
