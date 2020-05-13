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

## Pipeline Infrastructure

The implementation of the pipeline is designed around the [Bonobo](https://www.bonobo-project.org) framework.
This provides an API for describing data transformation as a data flow graph which each node is a function or class that when invoked accepts a single record as input and emits zero or more output records.
These nodes are connected so that the modeling of data is built up from initial records (either a row from a CSV file or a sub-tree from an XML document), and data is written to output files at every leaf node in the graph (and in some cases also at internal nodes).
In our case, the output is always JSON-LD files produced by the [cromulent](https://github.com/thegetty/crom) framework.

* @@ TODO: use bonobo API, but found the overhead of using its implementation to run the pipeline was too expensive, so wrote an API compatible custom execution module
* @@ TODO: need for breaking the graph into ordered components
* @@ TODO: 
* @@ TODO: 

## Code and File Organization

* @@ TODO: bonobo-compatible evaluation code
* @@ TODO: utilities
* @@ TODO: linkedart
* @@ TODO: projects

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

The pipeline code and tooling has been constructed to allow it to run entirely on a virtual machine in AWS.
To do this, each project has a `run` script (in `scripts/`) which orchestrates a number of tasks:

* Pulls the most recent code in the `aws` branch from github
* Builds a Docker image with that code (via the `dockerimage` Makefile target)
* Runs the Docker image with directories mapped as volumes to support input, output, and supplementary "service" data files
  * Sync the project's input data from S3
  * Run the pipeline code which models the data in the input files and writes JSON-LD files to the output path (via the project-specific Makefile target)
  * Run post-processing scripts (e.g. for Sales, this is where object URIs are reconciled based on the post-sales data)
  * Produce a metadata file `meta.nq` which enumerates all of the named-graphs which will appear in the N-Quads output files
  * Transcode the JSON-LD files to produce corresponding N-Quads data files (via the `nq` Makefile target)
* Creates `.tar.gz` files with the output data
* Uploads those files to S3

NOTE: While effort has been put into making these scripts portable and reproducible, there are some hard-coded paths that remain in them. In particular, the location of the log files that are generated by these scripts is currently hard-coded, and will have to be updated to allow it to work under new accounts.

@@ TODO: Coordination of the uri-to-uuid map file that allows URIs to be consistent both across pipeline runs and between different project pipelines (e.g. from People and Sales)
