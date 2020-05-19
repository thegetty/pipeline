# Getty Linked Data Transformation Pipeline

## Table of Contents

* [Table of Contents](#table-of-contents)
* [Pipeline Infrastructure](#pipeline-infrastructure)
* [Code and File Organization](#code-and-file-organization)
* [Projects](#projects)
    * [Provenance Index](#provenance-index)
        * [Sales](#sales)
        * [Knoedler](#knoedler)
        * [People](#people)
    * [AATA](#aata)
* [Tooling and Post-pipeline Processing](#tooling-and-post-pipeline-processing)
	* [Running in AWS](#running-in-aws)
	* [Ensuring consistent UUIDs](#ensuring-consistent-uuids)
		* [Format of the URI to UUID Mapping File](#format-of-the-uri-to-uuid-mapping-file)
		* [Performance of URI to UUID Mapping](#performance-of-uri-to-uuid-mapping)
	
## Pipeline Infrastructure

The implementation of the pipeline is designed around the [Bonobo](https://www.bonobo-project.org) framework.
This provides an API for describing data transformation as a data flow graph which each node is a function or class that when invoked accepts a single record as input and emits zero or more output records.
These nodes are connected so that the modeling of data is built up from initial records (either a row from a CSV file or a sub-tree from an XML document), and data is written to output files at every leaf node in the graph (and in some cases also at internal nodes).
In our case, the output is always JSON-LD files produced by the [cromulent](https://github.com/thegetty/crom) framework.

## Code and File Organization

### Bonobo

While the code is designed based on Bonobo, we found that the actual runtime of the larger pipelines ([Sales](#sales), in particular) was slowed down by the design of Bonobo in which each graph node is implemented as its own thread with queues used to pass data between them.
At the time, no other execution strategies were reliably implemented in Bonobo.
Therefore, the code in `pipeline.execution` was added to implement a single-threaded evaluation of a Bonobo graph. It is not intended to implement 100% of the Bonobo API, but it implements a large subset that is sufficient to execute our pipeline graphs with higher performance.

In several of the pipelines described below, a single Bonobo data flow graph is not sufficient to correctly model the data due to data dependencies that require either synchronization between disparate sub-graphs, or because some part of the input data must be fully processed before another can use its output.
To address these needs, the pipelines are sometimes broken into multiple components (each an independent Bonobo graph).
The components are run sequentially and use Bonobo "services" as a way to pass data out of one component and into another.

### File Structure Overview

The overall structure of files in the repository:

* [`Dockerfile`](../Dockerfile) - Used to build the Docker image which allows the pipeline to run in AWS
* [`Makefile`](../Makefile) - Used in conjunction with [`data`](../data) to both build the Docker image as well as run code within that image
* [`aata.py`](../aata.py) - Script used to start the AATA pipeline
* [`data`](../data/) - Common and per-project data files (JSON files are loaded automatically as Bonobo "services")
* [`docs`](../docs/) - Documentation
* [`knoedler.py`](../knoedler.py) - Script used to start the Knoedler pipeline
* [`people.py`](../people.py) - Script used to start the People pipeline
* [`pipeline`](../pipeline/) - Python code
    * [`execution.py`](../pipeline/execution.py) - Bonobo-compatible, single-threaded graph evaluation
    * [`io`](../pipeline/io/) - Code related to file I/O
    * [`linkedart.py`](../pipeline/linkedart.py) - Shared code to create `cromulent` objects from python data dictionaries
    * [`nodes`](../pipeline/nodes/) - Common classes/functions that are used as Bonobo graph nodes
    * [`projects`](../pipeline/projects/) - Per-project pipeline implementation
    * [`provenance`](../pipeline/provenance/) - Shared base class used by the provenance project pipelines (Sales and Knoedler)
    * [`util`](../pipeline/util/) - Utility classes/functions used throughout the code
* [`requirements.txt`](../requirements.txt) - Python dependencies
* [`sales.py`](../sales.py) - Script used to start the Sales pipeline
* [`scripts`](../scripts/) - Scripts used in running the pipeline, and post-processing data
* [`settings.py`](../settings.py) - Some configuration values that are used in the pipeline code
* [`setup.py`](../setup.py) - Python package management
* [`tests`](../tests/) - Tests
* [`wsgi.py`](../wsgi.py) - A flask-based webserver to allow simple visualization of JSON-LD output files

## Projects

### Provenance Index

#### Sales

[![Diagram of the Sales transformation pipeline](sales-pipeline.jpg "Sales Transformation Pipeline")](sales-pipeline.pdf)

The Sales pipeline is divided into three, sequentially-run components:

1. Auction Events

	These are records of the auction events described by an auction catalog, and includes information on the event's location and the auction house which organized and/or ran the event.

	The records provide information on which "auction" records were not actual auctions, but were instead private sale events. This will be passed on to the second pipeline component via the `non_auctions` service data.
	
	The records also provide information about the events which will be needed in the modeling of information related to the auction lots (including dates, locations, experts/commissaires, and auction houses). This data will be passed on to the third pipeline component via the `event_properties` service data.

2. Physical Catalogs

	These are the known physical copies of the auction catalogs. There may be multiple known copies of a catalog, owned by one or more person/organization.
	
	The processing of this data leads to identifying which catalog numbers identify a single known physical catalog. This will be used in the third pipeline component to assert a known relationship between a hand-written note relating to an object and the physical auction catalog in which it was written. This data is passed to the third pipeline component via the `unique_catalogs` service data.

3. Sales Contents

	These are the records of objects and lots offered (and possibly sold) in each auction event (or private sale).
	The modeling of this data results in a description of the bidding and/or sale (or drawing in the case of lotteries), and the object being sold including its:
	
	* creation/artist(s)
	* destruction
	* objects it may have been copied after or influenced by
	* known current owner/location
	* previous owners
	* previous (and post) sales this object is known to have been included in
	
	The information about previous sales can lead to identifying the object in multiple records.
	In this case, the URI used to identify this object is reconciled across sales and provides a link in the output between a single object and multiple provenance entries in which it took part.
	Note that just having previous sale information is not sufficient to perform this reconciliation, as it may be the case that the identified previous sale was for a lot of multiple items, making it impossible to distinguish which is the item of interest.

#### Knoedler

[![Diagram of the Knoedler transformation pipeline](knoedler-pipeline.jpg "Knoedler Transformation Pipeline")](knoedler-pipeline.pdf)

The Knoedler pipeline processes a single input CSV file of stock/sales records.
Each record represents two provenance entries. In the common case, a record represents an "incoming" purchase of an item by Knoedler, and an "outgoing" sale of that item by Knoedler.
Each of these transactions can be in partnership with other buyers and/or sellers.

Depending on the value of the record's `transaction` field, the "outgoing" provenance entries may represent a sale, inventorying, a return, destruction, theft, or loss.

#### People

[![Diagram of the People transformation pipeline](people-pipeline.jpg "People Transformation Pipeline")](people-pipeline.pdf)

The People pipeline is relatively simple. It processes a single input CSV file of people (and organization) authority records, and produces JSON-LD output which models those people and organizations, and related locations.
Most of the modeling occurs in the shared code in `pipeline.projects.UtilityHelper.add_person` and `pipeline.linkedart.MakeLinkedArtPerson`.

### AATA

[![Diagram of the AATA transformation pipeline](aata-pipeline.jpg "AATA Transformation Pipeline")](aata-pipeline.pdf)

The AATA pipeline models abstract records and related information including:

* The textual works that are being abstracted (books, articles, technical reports), and their properties (ISBN, DOI, titles, notes, etc.)
* Issues and/or Journals, or Book Series that the work belonged to
* People and organizations related to the work (authors, publishers, distributors)
* Publishing and Distributing activities
* Places (relating to authors, publishing/distributing)
* Indexing and classification concepts/terms

The pipeline is divided into two, sequentially-run components:

* Places
* Everything else (Abstracts, Journals, Series, and authority records for People and Corporate Bodies)

The reason place data needs to be processed first is to allow consistent URI assignment for countries and states/provinces. These are often identified by an internal identifier, but sometimes only by a name.
To be consistent, all places authority records are modeled, and countries and states/provinces are assigned URIs based on their primary name and indexed by their internal ID (with this information being passed on to the second pipeline component via the `places_with_named_uris` service data).
Subsequently, places that are referenced by an internal ID are assigned a URI conditionally based on the entries present in the `places_with_named_uris` service data.

# Tooling and Post-pipeline Processing

## Running in AWS

The pipeline code and tooling has been constructed to allow it to run entirely on a virtual machine in AWS.
To do this, each project has a `run` script (in [`scripts/`](../scripts/)) which orchestrates a number of tasks:

* Pulls the most recent code in the `aws` branch from GitHub
* Builds a Docker image with that code (via the `dockerimage` Makefile target)
* Runs the Docker image with directories mapped as volumes to support input, output, and supplementary "service" data files
  * Sync the project's input data from S3
  * Run the pipeline code which models the data in the input files and writes JSON-LD files to the output path (via the project-specific Makefile target)
  * Run post-processing scripts (e.g. for Sales, this is where object URIs are reconciled based on the post-sales data)
  * Produce a metadata file `meta.nq` which enumerates all of the named-graphs which will appear in the N-Quads output files
  * Transcode the JSON-LD files to produce corresponding N-Quads data files (via the `nq` Makefile target)
* Creates `.tar.gz` files with the output data
* Uploads those files to S3

Running:

```
./scripts/runpipeline-sales.sh
```

will produce output in a directory whose name contains the current date, e.g. `/data/output/sales-2020-05-18`, and will upload files to S3 for the Provenance project:

```
/provenance_batch/output/sales/sales-2020-05-14-all.nq.gz
/provenance_batch/output/sales/sales-2020-05-14-jsonld.tar.gz
/provenance_batch/output/sales/sales-2020-05-14-meta.nq.gz
/provenance_batch/output/sales/sales-2020-05-14-nquads.tar.gz
```

NOTE: While effort has been put into making these scripts portable and reproducible, there are some hard-coded paths that remain in them. In particular, the location of the log files that are generated by these scripts is currently hard-coded, and will have to be updated to allow it to work under new accounts.

## Ensuring consistent UUIDs

Part of the post-processing that occurs is replacing some URIs in the data with UUID values to support the requirements of data imported to Arches.
The URIs which are replaced all begin with the prefix `"tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:"`.

For some projects (the Provenance projects of Sales, Knoedler, and People), we ensure that UUID values are consistent across multiple runs of the pipeline by using a persistent mapping from URIs to UUIDs.
This mapping is stored in S3; as specified in the `Makefile` it is located at `/provenance_batch/data/uri_to_uuid_map.json`.
Use of the same mapping file across the provenance projects allows URIs to be shared between projects (e.g. people records that appear in all three projects).

NOTE: The persistence of URIs is not automated. The mapping file in S3 is manually uploaded. This means that it persists only previously seen URIs, and to ensure consistent UUIDs of any new data, an updated mapping file must be uploaded to S3 before a new pipeline run starts.

### Format of the URI to UUID Mapping File

The URI to UUID mapping file is a JSON document containing a single object mapping URI suffixes to encoded UUID values.

The keys of the object, when prepended with the special `tag:` URI, result in the URI value to map to the UUID.
For example, `"shared#PERSON,AUTH,SCHOOF"` represents the URI `<tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SCHOOF>`.

The values of the object are UUID values, encoded in Base64.
This encoded is used to reduce the overall size of the JSON file due to memory pressure and problems that result in Python as the file size approaches 2GB (at the time of writing, May 2020, the size of the mapping file is roughly 1.6GB).
It was chosen as a reasonable balance of size reduction and portability (being easily decoded in both Python and Swift).
For example, `"mBy40hS4QhqKLb/fqqgPgg=="` represents the UUID `<urn:uuid:981cb8d2-14b8-421a-8a2d-bfdfaaa80f82>`.

### Performance of URI to UUID Mapping

The URI to UUID mapping process involves:

* reading all generated JSON-LD files
* matching all URIs with the special `tag:` URI prefix
* determining which matching URIs are not present in the persistent mapping file
* generating new UUID values for each such URI

Due to the performance-sensitive nature of this process, we found that a Python implementation was not suitable.
For this reason, a small Swift program ([`scripts/generate_uri_uuids.swift`](../scripts/generate_uri_uuids.swift)) was written to perform this task.
Swift was chosen because:

* it could concisely represented the task
* it was performant both as a compiled language and because it can take full advantage of hardware-based parallelism
* it was simple to add support in the Docker image

[`scripts/generate_uri_uuids.swift`](../scripts/generate_uri_uuids.swift) updates the URI to UUID mapping file in-place.
Once updated, the actual rewriting of URIs in the JSON-LD files is an embarrassingly parallel task (modulo the enumeration and partitioning of the JSON-LD files) and suitable for the Python implementation found in [`scripts/rewrite_uris_to_uuids_parallel.py`](../scripts/rewrite_uris_to_uuids_parallel.py).
