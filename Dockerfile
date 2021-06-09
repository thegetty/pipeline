FROM python:3.8
WORKDIR /usr/src/app

RUN pip install --no-cache-dir awscli

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ENV PATH="/usr/src/app:${PATH}"
ENV PYTHONPATH="/usr/src/app:${PYTHONPATH}"
ENV GETTY_PIPELINE_INPUT=/data
ENV GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH=/data/common
ENV GETTY_PIPELINE_OUTPUT=/output
ENV GETTY_PIPELINE_TMP_PATH=/tmp
ENV PYTHON=/usr/local/bin/python
ENV LC_ALL="C"
ENV LC_CTYPE="C"

COPY scripts scripts
COPY pipeline pipeline
COPY tests tests
COPY data/common /data/common
COPY Makefile setup.py aata.py sales.py knoedler.py people.py settings.py ./

FROM swift:latest
WORKDIR /usr/src/swift

RUN mkdir scripts
COPY Makefile ./
COPY scripts/find_matching_json_files.swift ./scripts/
RUN make scripts/find_matching_json_files

FROM python:3.8
WORKDIR /usr/src/app

RUN pip install --no-cache-dir awscli

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=0 /usr/src/app ./
COPY --from=1 /usr/src/swift/scripts/find_matching_json_files scripts/
COPY --from=1 /usr/lib/swift /usr/lib/swift

EXPOSE 8080
VOLUME ["/data"]
VOLUME ["/output"]
VOLUME ["/services"]
CMD [ "make", "sales", "LIMIT=100" ]
