LIMIT?=100
DEBUG?=1
DOT=dot
QUIET?=1

SHELL := /bin/bash

aata:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./aata.py

aatagraph: /tmp/aata.pdf
	open -a Preview /tmp/aata.pdf

nt:
	curl -s 'https://linked.art/ns/v1/linked-art.json' > /tmp/linked-art.json
	echo 'Transcoding JSON-LD to N-Triples...'
	find output -name '*.json' | sort | xargs -n 128 -P 10 ./json2nt.py /tmp/linked-art.json

pir:
	mkdir -p /tmp/pipeline
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./pir.py
	./rewrite_post_sales_uris.py "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"
	./rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID#'

pirgraph: /tmp/pir.pdf
	open -a Preview /tmp/pir.pdf

knoedler:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./knoedler.py

knoedlergraph: /tmp/knoedler.pdf
	open -a Preview /tmp/knoedler.pdf

upload:
	./upload_to_arches.py

test:
	python3 -B setup.py test

/tmp/aata.dot: aata.py
	./aata.py dot > /tmp/aata.dot

/tmp/aata.pdf: /tmp/aata.dot
	$(DOT) -Tpdf -o /tmp/aata.pdf /tmp/aata.dot

/tmp/pir.dot: pir.py
	./pir.py dot > /tmp/pir.dot

/tmp/knoedler.dot: knoedler.py
	./knoedler.py dot > /tmp/knoedler.dot

/tmp/pir.pdf: /tmp/pir.dot
	$(DOT) -Tpdf -o /tmp/pir.pdf /tmp/pir.dot
	
/tmp/knoedler.pdf: /tmp/knoedler.dot
	$(DOT) -Tpdf -o /tmp/knoedler.pdf /tmp/knoedler.dot
	
clean:
	rm -rf output/*
	rm -rf /tmp/pipeline/*
	rm -f /tmp/aata.pdf
	rm -f /tmp/aata.dot
	rm -f /tmp/pir.pdf
	rm -f /tmp/knoedler.pdf
	rm -f /tmp/pir.dot
	rm -f /tmp/knoedler.dot
	rm -f "${GETTY_PIPELINE_TMP_PATH}/post_sale_rewrite_map.json"

.PHONY: aata aatagraph knoedler knoedlergraph pir pirgraph test upload nt
