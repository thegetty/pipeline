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
	docker run -t --env GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=$(GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH) --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -v$(GETTY_PIPELINE_INPUT):/data -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make pir nt

cleandocker: dockerimage
	docker run -t --env AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) --env AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make fetch aata nt

dockertest: dockerimage
	docker run -t -v$(GETTY_PIPELINE_INPUT):/data -v$(GETTY_PIPELINE_OUTPUT):/output pipeline make test

dockerimage: Dockerfile
	docker build -t pipeline .

fetch: fetchaata fetchpir fetchknoedler

fetchaata:
	mkdir -p $(GETTY_PIPELINE_INPUT)/aata
	aws s3 sync s3://jpgt-or-provenance-01/provenance_batch/data/aata $(GETTY_PIPELINE_INPUT)/aata

fetchpir:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	mkdir -p $(GETTY_PIPELINE_INPUT)/provenance
	aws s3 sync s3://jpgt-or-provenance-01/provenance_batch/data/provenance $(GETTY_PIPELINE_INPUT)/provenance
	cp $(GETTY_PIPELINE_INPUT)/provenance/uri_to_uuid_map.json "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json"

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
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | sort | xargs -n 128 -P 10 $(PYTHON) ./scripts/json2nt.py $(GETTY_PIPELINE_TMP_PATH)/linked-art.json

nq: jsonlist
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)
	curl -s 'https://linked.art/ns/v1/linked-art.json' > $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	echo 'Transcoding JSON-LD to N-Quads...'
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | xargs -n 256 -P 16 $(PYTHON) ./scripts/json2nq.py $(GETTY_PIPELINE_TMP_PATH)/linked-art.json
	find $(GETTY_PIPELINE_OUTPUT) -name '[0-9a-f][0-9a-f]*.nq' | xargs -n 256 cat | gzip - > $(GETTY_PIPELINE_OUTPUT)/all.nq.gz
	gzip -k $(GETTY_PIPELINE_OUTPUT)/meta.nq

pirdata:
	mkdir -p $(GETTY_PIPELINE_TMP_PATH)/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) $(PYTHON) ./pir.py
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_post_sales_uris.py "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:' "${GETTY_PIPELINE_TMP_PATH}/uri_to_uuid_map.json"
	ls $(GETTY_PIPELINE_OUTPUT) | PYTHONPATH=`pwd` xargs -n 1 -P 8 -I '{}' $(PYTHON) ./scripts/coalesce_json.py "${GETTY_PIPELINE_OUTPUT}/{}"
	PYTHONPATH=`pwd` $(PYTHON) ./scripts/remove_meaningless_ids.py
	# Reorganizing JSON files...
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' | PYTHONPATH=`pwd` xargs -n 256 -P 16 $(PYTHON) ./scripts/reorganize_json.py
	# Done
	find $(GETTY_PIPELINE_OUTPUT) -type d -empty -delete

jsonlist:
	find $(GETTY_PIPELINE_OUTPUT) -name '*.json' > $(GETTY_PIPELINE_TMP_PATH)/json_files.txt

pir: pirdata jsonlist
	cat $(GETTY_PIPELINE_TMP_PATH)/json_files.txt | PYTHONPATH=`pwd` $(PYTHON) ./scripts/generate_metadata_graph.py sales

pirgraph: $(GETTY_PIPELINE_TMP_PATH)/pir.pdf
	open -a Preview $(GETTY_PIPELINE_TMP_PATH)/pir.pdf

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

$(GETTY_PIPELINE_TMP_PATH)/pir.dot: pir.py
	./pir.py dot > $(GETTY_PIPELINE_TMP_PATH)/pir.dot

$(GETTY_PIPELINE_TMP_PATH)/knoedler.dot: knoedler.py
	./knoedler.py dot > $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

$(GETTY_PIPELINE_TMP_PATH)/pir.pdf: $(GETTY_PIPELINE_TMP_PATH)/pir.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/pir.pdf $(GETTY_PIPELINE_TMP_PATH)/pir.dot

$(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf: $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot
	$(DOT) -Tpdf -o $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot

clean:
	rm -rf $(GETTY_PIPELINE_OUTPUT)/*
	rm -rf $(GETTY_PIPELINE_TMP_PATH)/pipeline/*
	rm -f $(GETTY_PIPELINE_TMP_PATH)/json_files.txt
	rm -f $(GETTY_PIPELINE_TMP_PATH)/aata.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/aata.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/pir.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/knoedler.pdf
	rm -f $(GETTY_PIPELINE_TMP_PATH)/pir.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/knoedler.dot
	rm -f $(GETTY_PIPELINE_TMP_PATH)/sales-tree.data
	rm -f "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"

.PHONY: aata aatagraph knoedler knoedlergraph pir pirgraph test upload nt docker dockerimage dockertest fetch fetchaata fetchpir fetchknoedler jsonlist pirdata
