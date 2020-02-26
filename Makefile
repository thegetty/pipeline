LIMIT?=100
DEBUG?=1
DOT=dot
QUIET?=1
PYTHON?=python3
GETTY_PIPELINE_OUTPUT?=`pwd`/output
GETTY_PIPELINE_INPUT?=`pwd`/data
GETTY_PIPELINE_TMP_PATH?=/tmp
GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH?=`pwd`/data/common

SHELL := /bin/bash

docker: dockerimage
	docker run -t --env GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=$(GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH) --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -v$(GETTY_PIPELINE_INPUT):/data -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make sales nt

cleandocker: dockerimage
	docker run -t --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make fetch aata nt

dockertest: dockerimage
	docker run -t -v$(GETTY_PIPELINE_INPUT):/data -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make test

dockerimage: Dockerfile
	docker build -t pipeline .

fetch: fetchaata fetchsales fetchknoedler

fetchaata:
	mkdir -p $(GETTY_PIPELINE_INPUT)/aata
	aws s3 sync s3://jpgt-or-provenance-01/provenance_batch/data/aata $(GETTY_PIPELINE_INPUT)/aata

fetchsales:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	mkdir -p $(GETTY_PIPELINE_INPUT)/sales
	aws s3 sync s3://jpgt-or-provenance-01/provenance_batch/data/provenance $(GETTY_PIPELINE_INPUT)/sales
	cp $(GETTY_PIPELINE_INPUT)/sales/uri_to_uuid_map.json "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json"

fetchknoedler:
	mkdir -p $(GETTY_PIPELINE_INPUT)/knoedler
	aws s3 sync s3://jpgt-or-provenance-01/provenance_batch/data/knoedler $(GETTY_PIPELINE_INPUT)/knoedler

aata:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./aata.py
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'

aatagraph: $(GETTY_PIPELINE_TMP_PATH)/aata.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/aata.pdf

nt: jsonlist
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)
	curl -s 'https://linked.art/ns/v1/linked-art.json' > $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	echo 'Transcoding JSON-LD to N-Triples...'
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | sort | xargs -n 128 -P 16 $(PYTHON) ./scripts/json2nt.py $(GETTY_PIPELINE_TMP_PATH)/linked-art.json

nq: jsonlist
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)
	curl -s 'https://linked.art/ns/v1/linked-art.json' > $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	echo 'Transcoding JSON-LD to N-Quads...'
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | xargs -n 256 -P 16 $(PYTHON) ./scripts/json2nq.py $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	find $(GETTY_PIPELINE_OUTPUT) -name '[0-9a-f][0-9a-f]*.nq' | xargs -n 256 cat | gzip - > $(GETTY_PIPELINE_OUTPUT)/all.nq.gz
	gzip -k $(GETTY_PIPELINE_OUTPUT)/meta.nq

salespipeline:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./sales.py

scripts/generate_uri_uuids: scripts/generate_uri_uuids.swift
	swiftc scripts/generate_uri_uuids.swift -o scripts/generate_uri_uuids

scripts/find_matching_json_files: scripts/find_matching_json_files.swift
	swiftc scripts/find_matching_json_files.swift -o scripts/find_matching_json_files

salespostsalefilelist: scripts/find_matching_json_files
	time ./scripts/find_matching_json_files "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json" $(GETTY_PIPELINE_OUTPUT) > $(GETTY_PIPELINE_OUTPUT)/post-sale-matching-files.txt

salespostsalerewrite: salespostsalefilelist
	cat $(GETTY_PIPELINE_OUTPUT)/post-sale-matching-files.txt | PYTHONPATH=`pwd`  xargs -n 256 $(PYTHON) ./scripts/rewrite_post_sales_uris.py "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"

postprocessing_uuidmap: ./scripts/generate_uri_uuids
	./scripts/generate_uri_uuids "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json" $(GETTY_PIPELINE_OUTPUT) 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:'

postprocessing_rewrite_uris:
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids_parallel.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:' "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json"

salespostprocessing: salespostsalerewrite postprocessing_uuidmap postprocessing_rewrite_uris
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P 16 -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/remove_meaningless_ids.py
	# Reorganizing JSON files...
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' | PYTHONPATH=`pwd` xargs -n 256 -P 16 $(PYTHON) ./scripts/reorganize_json.py

salesdata: salespipeline salespostprocessing
	find $(GETTY_PIPELINE_OUTPUT) -type d -empty -delete

jsonlist:
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' > $(GETTY_PIPELINE_TMP_PATH)/json_files.txt

sales: salesdata jsonlist
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | PYTHONPATH=`pwd` $(PYTHON) ./scripts/generate_metadata_graph.py sales

salesgraph: $(GETTY_PIPELINE_TMP_PATH)/sales.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/sales.pdf

knoedler:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./knoedler.py
# 	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:knoedler:REPLACE-WITH-UUID#'

knoedlergraph: $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf

upload:
	./upload_to_arches.py

test:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline_tests
	python3 -B setup.py test

$(GETTY_PIPELINE_TMP_PATH)/aata.dot: aata.py
	./aata.py dot > $(GETTY_PIPELINE_TMP_PATH)/aata.dot

$(GETTY_PIPELINE_TMP_PATH)/aata.pdf: $(GETTY_PIPELINE_TMP_PATH)/aata.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/aata.pdf $(GETTY_PIPELINE_TMP_PATH)/aata.dot

$(GETTY_PIPELINE_TMP_PATH)/sales.dot: sales.py
	./sales.py dot > $(GETTY_PIPELINE_TMP_PATH)/sales.dot

$(GETTY_PIPELINE_TMP_PATH)/knoedler.dot: knoedler.py
	./knoedler.py dot > $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

$(GETTY_PIPELINE_TMP_PATH)/sales.pdf: $(GETTY_PIPELINE_TMP_PATH)/sales.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/sales.pdf $(GETTY_PIPELINE_TMP_PATH)/sales.dot

$(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf: $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

clean:
	rm -rf $(GETTY_PIPELINE_OUTPUT)/*
	rm -rf $(GETTY_PIPELINE_TMP_PATH)/pipeline/*
	rm -f $(GETTY_PIPELINE_TMP_PATH)/json_files.txt
	rm -f $(GETTY_PIPELINE_TMP_PATH)/aata.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/aata.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/sales.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/sales.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/sales-tree.data
	rm -f "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"

.PHONY: aata aatagraph knoedler knoedlergraph sales salesgraph test upload nt docker dockerimage dockertest fetch fetchaata fetchsales fetchknoedler jsonlist salesdata salespipeline salespostprocessing salespostsalefilelist postprocessing_uuidmap postprocessing_rewrite_uris
