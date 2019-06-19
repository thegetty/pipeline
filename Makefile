LIMIT?=100
DEBUG?=1
DOT=dot
QUIET?=1

aata:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./aata.py

aatagraph: /tmp/aata.pdf
	open -a Preview /tmp/aata.pdf

pir:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./pir.py
	./rewrite_uris_to_uuids.py 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID#'

pirgraph: /tmp/pir.pdf
	open -a Preview /tmp/pir.pdf

knoedler:
	QUIET=$(QUIET) GETTY_PIPELINE_DEBUG=$(DEBUG) GETTY_PIPELINE_LIMIT=$(LIMIT) time ./knoedler.py

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

/tmp/pir.pdf: /tmp/pir.dot
	$(DOT) -Tpdf -o /tmp/pir.pdf /tmp/pir.dot
	
clean:
	rm -rf output/*
	rm -f /tmp/aata.pdf
	rm -f /tmp/aata.dot
	rm -f /tmp/pir.pdf
	rm -f /tmp/pir.dot

.PHONY: aata knoedler pir pirgraph test aatagraph aata.py upload pir.py aata.py
