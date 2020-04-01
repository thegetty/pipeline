LIMIT?=100
CONCURRENCY?=16
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
	docker run -t --env GETTY_PIPELINE_LIMIT=$(LIMIT) --env GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=/services/common --env GETTY_PIPELINE_SERVICE_FILES_PATH=/services --env GETTY_PIPELINE_INPUT=/data --env GETTY_PIPELINE_OUTPUT=/output --env GETTY_PIPELINE_TMP_PATH=/output/tmp --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -v$(GETTY_PIPELINE_INPUT):/data:Z -v$(GETTY_PIPELINE_OUTPUT):/output:Z -v$(GETTY_PIPELINE_INPUT):/services:Z -it pipeline make clean sales

cleandocker: dockerimage
	docker run -t --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make fetch aata nt

dockertest: dockerimage
	docker run -t -v$(GETTY_PIPELINE_INPUT):/data -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make test

dockerimage: Dockerfile
	docker build -t pipeline .

### FETCH DATA FROM S3

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

### SHARED POST-PROCESSING

nt: jsonlist
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)
	curl -s 'https://linked.art/ns/v1/linked-art.json' > $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	split -n r/$(CONCURRENCY) $(GETTY_PIPELINE_TMP_PATH)/json_files.txt "${GETTY_PIPELINE_TMP_PATH}/json_files.chunk."
	echo 'Transcoding JSON-LD to N-Triples...'
	ls $(GETTY_PIPELINE_TMP_PATH)/json_files.chunk.* | xargs -n 1 -P $(CONCURRENCY) $(PYTHON) ./scripts/json2nt.py -c $(GETTY_PIPELINE_TMP_PATH)/linked-art.json -l
	rm $(GETTY_PIPELINE_TMP_PATH)/json_files.chunk.*

nq: jsonlist
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)
	curl -s 'https://linked.art/ns/v1/linked-art.json' > $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	split -n r/$(CONCURRENCY) $(GETTY_PIPELINE_TMP_PATH)/json_files.txt "${GETTY_PIPELINE_TMP_PATH}/json_files.chunk."
	echo 'Transcoding JSON-LD to N-Quads...'
	ls $(GETTY_PIPELINE_TMP_PATH)/json_files.chunk.* | xargs -n 1 -P $(CONCURRENCY) $(PYTHON) ./scripts/json2nq.py -c $(GETTY_PIPELINE_TMP_PATH)/linked-art.json -l
	find $(GETTY_PIPELINE_OUTPUT) -name '[0-9a-f][0-9a-f]*.nq' | xargs -n 256 cat | gzip - > $(GETTY_PIPELINE_OUTPUT)/all.nq.gz
	gzip -k $(GETTY_PIPELINE_OUTPUT)/meta.nq
	rm $(GETTY_PIPELINE_TMP_PATH)/json_files.chunk.*

postprocessjson:
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P 8 -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"

scripts/generate_uri_uuids: scripts/generate_uri_uuids.swift
	swiftc scripts/generate_uri_uuids.swift -o scripts/generate_uri_uuids

scripts/find_matching_json_files: scripts/find_matching_json_files.swift
	swiftc scripts/find_matching_json_files.swift -o scripts/find_matching_json_files

postprocessing_uuidmap: ./scripts/generate_uri_uuids
	./scripts/generate_uri_uuids "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json" $(GETTY_PIPELINE_OUTPUT) 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:'

postprocessing_rewrite_uris:
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids_parallel.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:' "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json"

jsonlist:
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' > $(GETTY_PIPELINE_TMP_PATH)/json_files.txt

### AATA

aata:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./aata.py
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'

aatagraph: $(GETTY_PIPELINE_TMP_PATH)/aata.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/aata.pdf

$(GETTY_PIPELINE_TMP_PATH)/aata.dot: aata.py
	./aata.py dot > $(GETTY_PIPELINE_TMP_PATH)/aata.dot

$(GETTY_PIPELINE_TMP_PATH)/aata.pdf: $(GETTY_PIPELINE_TMP_PATH)/aata.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/aata.pdf $(GETTY_PIPELINE_TMP_PATH)/aata.dot

### PEOPLE

people: peopledata jsonlist
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | PYTHONPATH=`pwd` $(PYTHON) ./scripts/generate_metadata_graph.py people

peopledata: peoplepipeline peoplepostprocessing
	find $(GETTY_PIPELINE_OUTPUT) -type d -empty -delete

peoplepipeline:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./people.py

peoplepostprocessing: postprocessing_uuidmap postprocessing_rewrite_uris
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P $(CONCURRENCY) -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/remove_meaningless_ids.py
	# Reorganizing JSON files...
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' | PYTHONPATH=`pwd` xargs -n 256 -P $(CONCURRENCY) $(PYTHON) ./scripts/reorganize_json.py

peoplepostsalefilelist: scripts/find_matching_json_files
	time ./scripts/find_matching_json_files "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json" $(GETTY_PIPELINE_OUTPUT) > $(GETTY_PIPELINE_OUTPUT)/post-sale-matching-files.txt

peoplegraph: $(GETTY_PIPELINE_TMP_PATH)/people.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/people.pdf

$(GETTY_PIPELINE_TMP_PATH)/people.dot: people.py
	./people.py dot > $(GETTY_PIPELINE_TMP_PATH)/people.dot

$(GETTY_PIPELINE_TMP_PATH)/people.pdf: $(GETTY_PIPELINE_TMP_PATH)/people.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/people.pdf $(GETTY_PIPELINE_TMP_PATH)/people.dot


### SALES

sales: salesdata jsonlist
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | PYTHONPATH=`pwd` $(PYTHON) ./scripts/generate_metadata_graph.py sales

salesprofile:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) -m cProfile -o $(GETTY_PIPELINE_OUTPUT)/pipeline.prof ./sales.py
	snakeviz $(GETTY_PIPELINE_OUTPUT)/pipeline.prof
# 	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) -m flamegraph -o $(GETTY_PIPELINE_OUTPUT)/pipeline.flame.log ./sales.py
# 	perl ~/data/prog/ext/FlameGraph/flamegraph.pl --title "Sales Pipeline" $(GETTY_PIPELINE_OUTPUT)/pipeline.flame.log > $(GETTY_PIPELINE_OUTPUT)/pipeline.flame.svg

salesdata: salespipeline salespostprocessing
	find $(GETTY_PIPELINE_OUTPUT) -type d -empty -delete

salespipeline:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./sales.py

salespostprocessing: salespostsalerewrite postprocessing_uuidmap postprocessing_rewrite_uris
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P $(CONCURRENCY) -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/remove_meaningless_ids.py
	# Reorganizing JSON files...
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' | PYTHONPATH=`pwd` xargs -n 256 -P $(CONCURRENCY) $(PYTHON) ./scripts/reorganize_json.py

salespostsalerewrite: salespostsalefilelist
	cat $(GETTY_PIPELINE_OUTPUT)/post-sale-matching-files.txt | PYTHONPATH=`pwd`  xargs -n 256 $(PYTHON) ./scripts/rewrite_post_sales_uris.py "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"

salespostsalefilelist: scripts/find_matching_json_files
	time ./scripts/find_matching_json_files "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json" $(GETTY_PIPELINE_OUTPUT) > $(GETTY_PIPELINE_OUTPUT)/post-sale-matching-files.txt

salesgraph: $(GETTY_PIPELINE_TMP_PATH)/sales.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/sales.pdf

$(GETTY_PIPELINE_TMP_PATH)/sales.dot: sales.py
	./sales.py dot > $(GETTY_PIPELINE_TMP_PATH)/sales.dot

$(GETTY_PIPELINE_TMP_PATH)/sales.pdf: $(GETTY_PIPELINE_TMP_PATH)/sales.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/sales.pdf $(GETTY_PIPELINE_TMP_PATH)/sales.dot


### KNOEDLER

knoedler: knoedlerdata jsonlist
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | PYTHONPATH=`pwd` $(PYTHON) ./scripts/generate_metadata_graph.py knoedler

knoedlerdata: knoedlerpipeline knoedlerpostprocessing
	find $(GETTY_PIPELINE_OUTPUT) -type d -empty -delete

knoedlerpipeline:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./knoedler.py

knoedlerpostprocessing: postprocessing_uuidmap postprocessing_rewrite_uris
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P $(CONCURRENCY) -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/remove_meaningless_ids.py
	# Reorganizing JSON files...
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' | PYTHONPATH=`pwd` xargs -n 256 -P $(CONCURRENCY) $(PYTHON) ./scripts/reorganize_json.py

knoedlergraph: $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf

$(GETTY_PIPELINE_TMP_PATH)/knoedler.dot: knoedler.py
	./knoedler.py dot > $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

$(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf: $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

#######################

upload:
	./upload_to_arches.py

test:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline_tests
	python3 -B setup.py test

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

.PHONY: fetch fetchaata fetchsales fetchknoedler
.PHONY: aata aatagraph
.PHONY: knoedler knoedlergraph
.PHONY: people peoplegraph peopledata peoplepipeline peoplepostprocessing peoplepostsalefilelist
.PHONY: sales salesgraph salesdata salespipeline salespostprocessing salespostsalefilelist
.PHONY: test upload nt docker dockerimage dockertest jsonlist postprocessing_uuidmap postprocessing_rewrite_uris postprocessjson
