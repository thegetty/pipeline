import csv
import functools
import sys
import timeit
import warnings
from collections import defaultdict
from contextlib import suppress

import bonobo
from bonobo.config import Configurable, Option, Service
from cromulent import model, vocab

import settings
from pipeline.io.csv import CurriedCSVReader
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
from pipeline.linkedart import (
    MakeLinkedArtHumanMadeObject,
    MakeLinkedArtLinguisticObject,
    MakeLinkedArtOrganization,
    MakeLinkedArtPerson,
    PopulateObject,
    add_crom_data,
    get_crom_object,
    get_crom_objects,
)
from pipeline.nodes.basic import KeyManagement, RecordCounter
from pipeline.projects import PersonIdentity, PipelineBase, UtilityHelper
from pipeline.projects.knoedler import add_crom_price
from pipeline.provenance import ProvenanceBase
from pipeline.util import (
    ExtractKeyedValue,
    ExtractKeyedValues,
    GraphListSource,
    MatchingFiles,
    strip_key_prefix,
)
from pipeline.util.cleaners import parse_location_name


def filter_empty_book(data: dict, _):
    return data if data.get("no") else None


class GoupilPersonIdentity(PersonIdentity):
    pass


def record_id(data):
    no = data["no"]
    gno = data["gno"]
    page = data["pg"]
    row = data["row"]

    return (no, gno, page, row)


class GoupilProvenance:
    def add_goupil_creation_data(self, data: tuple):
        thing_label, _ = data["label"]

        goupil = self.helper.static_instances.get_instance("Group", "goupil")
        # TODO does goupil also have a place?
        # ny = self.helper.static_instances.get_instance('Place', 'newyork')

        o = get_crom_object(data)
        creation = model.Creation(ident="", label=f"Creation of {thing_label}")
        creation.carried_out_by = goupil
        # creation.took_place_at = ny
        o.created_by = creation

        return creation

    def model_object_artists(self, data: dict, artists: dict):

        for seq_no, a_data in enumerate(artists):
            auth_name = a_data.get("auth_name")
            ulan = a_data.get("ulan_id")
            name = a_data.get("name")
            nationality = a_data.get("nationality")
            places = []
            if a_data.get("location"):
                places.append(a_data.get("location"))

            if a_data.get("auth_location"):
                places.append(a_data.get("auth_location"))

            mod_notes = []
            if a_data.get("attrib_mod"):
                mod_notes.append(vocab.Note(content=a_data.get("attrib_mod")))

            if a_data.get("attrib_mod_auth"):
                mod_notes.append(vocab.Note(content=a_data.get("attrib_mod_auth")))

            a_data.update(
                {
                    "ulan": ulan,
                    "label": auth_name,
                    "role_label": "artist",
                    # "places": places,
                    "referred_to_by": mod_notes,
                }
            )

            artist = self.helper.add_person(
                a_data, record=get_crom_objects(data["_text_rows"]), relative_id=f"artist-{seq_no}"
            )

    def model_artists_with_modifers(self, data: dict, hmo: dict):
        # mofifiers are not yet to be modelled but we leave this function here as a placeholder

        artists = data.get("_artists", [])
        for a in artists:
            # might update the dict here
            pass

        self.model_object_artists(data, artists)

        return data

    def _prov_entry(
        self,
        data,
        date_key,
        participants,
        price_info=None,
        knoedler_price_part=None,
        shared_people=None,
        incoming=False,
        purpose=None,
        buy_sell_modifiers=None,
    ):
        data.setdefault("_people", [])

        for seq_no, p_data in enumerate(participants):
            auth_name = p_data.get("auth_name")
            ulan = p_data.get("ulan_id")
            places = [p_data.get("location"), p_data.get("auth_location")]
            places = []
            if p_data.get("location"):
                places.append(p_data.get("location"))

            if p_data.get("auth_location"):
                places.append(p_data.get("auth_location"))

            for place in places:
                current = parse_location_name(place, uri_base=self.helper.uid_tag_prefix)
                place_data = self.helper.make_place(current)
                data["_object"]["_locations"].append(place_data)

            mods_note = None
            if p_data.get("auth_mod"):
                mods_note = vocab.Note(content=p_data.get("auth_mod"))

            p_data.update(
                {
                    "ulan": ulan,
                    "referred_to_by": [mods_note],
                }
            )
            person = self.helper.add_person(
                p_data, record=get_crom_objects(data["_text_rows"]), relative_id=f"person-{seq_no}"
            )
            add_crom_data(p_data, person)
            data["_people"].append(p_data)

        if shared_people == None:
            shared_people = []

        if shared_people:
            for i, p in enumerate(shared_people):
                role = "shared-own"
                p_data = self.helper.copy_source_information(p, data)
                person = self.helper.add_person(
                    p_data, record=get_crom_objects(data["_text_rows"]), relative_id=f"{role}_{i+1}"
                )
                add_crom_data(p_data, person)
                data["_people"].append(p_data)

    def add_incoming_tx(self, data, buy_sell_modifiers):
        price_info = data.get("purchase")
        sellers = data["sellers"]
        for person in sellers:
            self.helper.copy_source_information(person, data)

        tx = self._prov_entry(data, "entry_date", sellers, shared_people=data.get("co_owners", []))

        prev_owner = data.get("prev_own")

        if prev_owner:
            self.model_prev_post_owners(data, [prev_owner], tx, "prev_own")

    def add_outging_tx(self, data, buy_sell_modifiers):
        price_info = data.get("purchase")
        buyers = data["buyers"]

        for person in buyers:
            self.helper.copy_source_information(person, data)

        tx = self._prov_entry(data, "entry_date", buyers)

        post_owner = data.get("post_own")

        if post_owner:
            self.model_prev_post_owners(data, [post_owner], tx, "post_own")

    def model_prev_post_owners(self, data, owners, tx, role, lot_object_key=None):
        for i, p in enumerate(owners):
            p_data = self.helper.copy_source_information(p, data)
            person = self.helper.add_person(p_data, record=None, relative_id=f"{role}_{i+1}")
            add_crom_data(p_data, person)
            data["_people"].append(p_data)


class ModelSale(Configurable, GoupilProvenance):
    """ """

    helper = Option(required=True)
    make_la_person = Service("make_la_person")

    def __call__(self, data: dict, make_la_person, buy_sell_modifiers=None, in_tx=None, out_tx=None):
        sellers = data["sellers"]
        buyers = data["buyers"]

        if not in_tx:
            if len(sellers):
                in_tx = self.add_incoming_tx(data, buy_sell_modifiers)
            else:
                pass

        if not out_tx:
            if len(buyers):
                out_tx = self.add_outging_tx(data, buy_sell_modifiers)
            else:
                pass

        yield data


class AddArtists(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_person = Service("make_la_person")

    def add_properties(self, data: dict, a: dict):
        a.update({"pi_record_no": data["pi_record_no"]})

    def __call__(self, data: dict, *, make_la_person):
        hmo = get_crom_object(data["_object"])

        # nice trick, might keep
        data["_record"] = data["_object"]

        self.model_artists_with_modifers(data, hmo)
        return data


class GoupilUtilityHelper(UtilityHelper):
    """
    Project-specific code for accessing and interpreting goupil data.
    """

    def __init__(self, project_name, static_instances=None):
        super().__init__(project_name)
        self.person_identity = GoupilPersonIdentity(
            make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri
        )
        self.uid_tag_prefix = self.proj_prefix

    def make_object_uri(self, pi_rec_no, *uri_key):
        uri_key = list(uri_key)
        uri = self.make_proj_uri(*uri_key)
        return uri

    def add_person(self, data, record: None, relative_id, **kwargs):
        self.person_identity.add_uri(data, record_id=relative_id)
        person = super().add_person(data, record=record, relative_id=relative_id, **kwargs)
        return person

    def copy_source_information(self, dst: dict, src: dict):
        if not dst or not isinstance(dst, dict):
            return dst
        for k in ["pi_record_no"]:
            with suppress(KeyError):
                dst[k] = src[k]
        return dst


class PopulateGoupilObject(Configurable, PopulateObject):
    helper = Option(required=True)
    make_la_org = Service("make_la_or")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, data: dict, *, make_la_org):
        data.setdefault("_object", {})
        data["_object"].setdefault("_organizations", [])
        data["_object"].setdefault("_locations", [])
        odata = data["object"]
        data["_object"].update({k: v for k, v in odata.items() if k in ("present_location")})

        self._populate_object_present_location(data["_object"])

        return data

    def _populate_object_present_location(self, data: dict):
        present_location = data.get("present_location")

        if present_location:
            loc = present_location.get("geog")

            if loc:
                current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
                inst = present_location.get("inst")
                if inst:
                    ulan = None
                    # TODO remove the following in time
                    ulan_id = present_location.get("ulan_id")
                    with suppress(ValueError, KeyError):
                        if ulan_id:
                            ulan = int(present_location.get("ulan_id"))

                    label = f"{inst} ({loc})"
                    uri = (
                        self.helper.make_proj_uri("ORG", "ULAN", ulan)
                        if ulan
                        else self.helper.make_proj_uri("ORG", "NAME", inst, "PLACE", loc)
                    )
                else:
                    import pprint

                    warnings.warn(
                        f"*** Object present location data has a location, but not an institution: {pprint.pformat(data)}"
                    )
                    label = "(Anonymous organization)"
                    uri = self.helper.make_proj_uri("ORG", "CURR-OWN", loc)

                org_data = {
                    "label": label,
                    "uri": uri,
                    "identifiers": [vocab.PrimaryName(ident="", content=inst)],
                    "ulan": ulan,
                }

                current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
                # link to hmo here
                place_data = self.helper.make_place(current)
                make_la_org = MakeLinkedArtOrganization()
                org_data = make_la_org(org_data)

                # TODO org place?
                data["_locations"].append(place_data)
                data["_organizations"].append(org_data)
            else:
                pass


class AddBooks(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_lo = Service("make_la_lo")
    make_la_hmo = Service("make_la_hmo")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo):
        books = data.get("_book_records", [])

        for seq_no, b_data in enumerate(books):
            book_id, gno, page, row = record_id(b_data)

            book_type = model.Type(ident="http://vocab.getty.edu/aat/300028051", label="Book")
            book_type.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300444970", label="Form")
            label = f"Goupil Stock Book {book_id}"

            book = {
                "uri": self.helper.make_proj_uri("Text", "Book", book_id),
                "object_type": vocab.AccountBookText,
                "classified_as": [book_type],
                "label": (label, vocab.instances["english"]),
                "identifiers": [self.helper.goupil_number_id(book_id, id_class=vocab.BookNumber)],
            }

            make_la_lo(book)
            self.add_goupil_creation_data(book)
            b_data.update(book)

        return data


class AddPages(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_lo = Service("make_la_lo")
    make_la_hmo = Service("make_la_hmo")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo):
        books = data.get("_book_records", [])
        data.setdefault("_text_pages", [])

        for seq_no, b_data in enumerate(books):
            book_id, _, page, _ = record_id(b_data)

            if not page:
                warnings.warn(
                    f"Record with id {data['pi_record_no']}, has book with id {book_id} but no page assosiated with it."
                )
                continue

            page_type = model.Type(ident="http://vocab.getty.edu/aat/300194222", label="Page")
            page_type.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300444970", label="Form")
            label = f"Goupil Stock Book {book_id}, Page {page}"

            page = {
                "uri": self.helper.make_proj_uri("Text", "Book", book_id, "Page", page),
                "object_type": vocab.AccountBookText,
                "classified_as": [page_type],
                "label": (label, vocab.instances["english"]),
                "identifiers": [self.helper.goupil_number_id(page, id_class=vocab.PageNumber)],
            }

            page.update({k: v for k, v in b_data.items() if k in ("no", "gno", "pg", "row")})
            make_la_lo(page)

            o_book = get_crom_object(b_data)
            o_page = get_crom_object(page)
            o_page.part_of = o_book

            data["_text_pages"].append(page)
            self.add_goupil_creation_data(page)

        return data


class AddRows(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_lo = Service("make_la_lo")
    make_la_hmo = Service("make_la_hmo")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo):
        pages = data.get("_text_pages", [])
        data.setdefault("_text_rows", [])

        notes = []
        for k in ("working_note", "verbatim_notes", "editor notes", "no_name_notes"):
            if data["object"].get(k):
                notes.append(vocab.Note(ident="", content=data["object"][k]))

        if data["object"].get("rosetta_handle"):
            notes.append(vocab.WebPage(ident=data["object"]["rosetta_handle"], label=data["object"]["rosetta_handle"]))

        for seq_no, p_data in enumerate(pages):
            book_id, _, page, row = record_id(p_data)

            if not row:
                warnings.warn(
                    f"Record with id {data['pi_record_no']}, has page with number {page} but no row assosiated with it."
                )
                continue

            row_type = model.Type(ident="http://vocab.getty.edu/aat/300438434", label="Entry")
            row_type.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300444970", label="Form")
            label = f"Goupil Stock Book {book_id}, Page {page}, Row {row}"

            row = {
                "uri": self.helper.make_proj_uri("Text", "Book", book_id, "Page", page, "Row", row),
                "object_type": vocab.AccountBookText,
                "classified_as": [row_type],
                "label": (label, vocab.instances["english"]),
                "identifiers": [
                    self.helper.goupil_number_id(
                        page,
                        id_class=vocab.RowNumber,
                        assignment_label="Entry Number Attribution by Goupil Gallery Ltd.",
                    ),
                    self.helper.gpi_number_id(data["pi_record_no"], vocab.StarNumber),
                ],
                "referred_to_by": notes,
            }

            make_la_lo(row)

            o_page = get_crom_object(p_data)
            o_row = get_crom_object(row)
            o_row.part_of = o_page

            data["_text_rows"].append(row)
            self.add_goupil_creation_data(row)

        return data


class GoupilPipeline(PipelineBase):
    """Bonobo-based pipeline for transforming goupil data from CSV into JSON-LD."""

    def __init__(self, input_path, data, **kwargs):
        project_name = "goupil"
        self.input_path = input_path
        self.services = None

        helper = GoupilUtilityHelper(project_name)
        super().__init__(project_name, helper=helper, verbose=kwargs.get("verbose", False))
        helper.static_instaces = self.static_instances

        # register project specific vocab here
        vocab.register_vocab_class(
            "BookNumber", {"parent": model.Identifier, "id": "300445021", "label": "Book Numbers"}
        )

        vocab.register_vocab_class(
            "PageNumber", {"parent": model.Identifier, "id": "300445022", "label": "Page Numbers"}
        )

        vocab.register_vocab_class(
            "RowNumber", {"parent": model.Identifier, "id": "300445023", "label": "Entry Number"}
        )

        self.graph = None
        self.models = kwargs.get("models", settings.arches_models)
        self.header_file = data["header_file"]
        self.files_pattern = data["files_pattern"]
        self.limit = kwargs.get("limit")
        self.debug = kwargs.get("debug", False)

        fs = bonobo.open_fs(input_path)
        with fs.open(self.header_file, newline="") as csvfile:
            r = csv.reader(csvfile)
            self.headers = [v.lower() for v in next(r)]

    def setup_services(self):
        services = super().setup_services()
        services.update(
            {
                # to avoid constructing new MakeLinkedArtPerson objects millions of times, this
                # is passed around as a service to the functions and classes that require it.
                "make_la_person": MakeLinkedArtPerson(),
                "make_la_lo": MakeLinkedArtLinguisticObject(),
                "make_la_hmo": MakeLinkedArtHumanMadeObject(),
                "make_la_org": MakeLinkedArtOrganization(),
                "counts": defaultdict(int),
            }
        )
        return services

    def add_sales_chain(self, graph, records, services, serialize=True):
        """Add transformation of sales records to the bonobo pipeline."""

        sales_records = graph.add_chain(
            KeyManagement(
                drop_empty=True,
                operations=[
                    {
                        "group_repeating": {
                            "_book_records": {
                                "rename_keys": {
                                    "stock_book_no": "no",
                                    "stock_book_gno": "gno",
                                    "stock_book_pg": "pg",
                                    "stock_book_row": "row",
                                },
                                "prefixes": ("stock_book_no", "stock_book_gno", "stock_book_pg", "stock_book_row"),
                                "postprocess": [filter_empty_book],
                            },
                            "_artists": {
                                "rename_keys": {
                                    "artist_name": "name",
                                    "art_authority": "auth_name",
                                    "attribution_mod": "attrib_mod",
                                    "attribution_auth_mod": "attrib_mod_auth",
                                    "artist_ulan_id": "ulan_id",
                                    "nationality": "nationality",
                                },
                                "prefixes": (
                                    "artist_name",
                                    "art_authority",
                                    "attribution_mod",
                                    "attribution_auth_mod",
                                    "artist_ulan_id",
                                    "nationality",
                                ),
                            },
                            "prices": {
                                "rename_keys": {
                                    "price_amount": "amount",
                                    "price_code": "code",
                                    "price_currency": "currency",
                                    "price_note": "note",
                                },
                                "prefixes": ("price_amount", "price_code", "price_currency", "price_note"),
                            },
                            "sellers": {
                                "rename_keys": {
                                    "seller_name": "name",
                                    "seller_loc": "location",
                                    "sell_auth_name": "auth_name",
                                    "sell_auth_loc": "auth_location",
                                    "sell_auth_mod": "auth_mod",
                                    "seller_ulan_id": "ulan_id",
                                },
                                "prefixes": (
                                    "seller_name",
                                    "seller_loc",
                                    "sell_auth_name",
                                    "sell_auth_loc",
                                    "sell_auth_mod",
                                    "seller_ulan_id",
                                ),
                            },
                            "co_owners": {
                                "rename_keys": {
                                    "joint_own": "name",
                                    "joint_own_sh": "share",
                                    "joint_ulan_id": "ulan_id",
                                },
                                "prefixes": ("joint_own", "joint_own_sh", "joint_ulan_id"),
                            },
                            "buyers": {
                                "rename_keys": {
                                    "buyer_name": "name",
                                    "buyer_loc": "location",
                                    "buy_auth_name": "auth_name",
                                    "buy_auth_addr": "auth_location",
                                    "buy_auth_mod": "auth_mod",
                                    "buyer_ulan_id": "ulan_id",
                                },
                                "prefixes": (
                                    "buyer_name",
                                    "buyer_loc",
                                    "buy_auth_name",
                                    "buy_auth_addr",
                                    "buy_auth_mod",
                                    "buyer_ulan_id",
                                ),
                            },
                        },
                        "group": {
                            "entry_date": {
                                "postprocess": lambda x, _: strip_key_prefix("entry_date_", x),
                                "properties": (
                                    "entry_date_year",
                                    "entry_date_month",
                                    "entry_date_day",
                                ),
                            },
                            "sale_date": {
                                "postprocess": lambda x, _: strip_key_prefix("sale_date_", x),
                                "properties": (
                                    "sale_date_year",
                                    "sale_date_month",
                                    "sale_date_day",
                                ),
                            },
                            "prev_own": {
                                "rename_keys": {
                                    "previous_owner": "name",
                                    "previous_sales": "sales",
                                },
                                "properties": (
                                    "previous_owner",
                                    "previous_sales",
                                ),
                            },
                            "post_own": {
                                "rename_keys": {
                                    "post_owner": "name",
                                    "post_sales": "sales",
                                },
                                "properties": (
                                    "post_owner",
                                    "post_sales",
                                ),
                            },
                            "purchase": {
                                "rename_keys": {
                                    "purch_amount": "amount",
                                    "purch_currency": "currency",
                                    "purch_note": "note",
                                    "purch_frame": "frame",
                                    "purch_code": "code",
                                    "purch_ques": "uncertain",
                                    "purch_loc": "location",
                                    "purch_loc_note": "location_note",
                                },
                                "postprocess": [
                                    # lambda d, p: add_crom_price(d, p, services)
                                ],  # use the one from knoedler for the time being
                                "properties": (
                                    "purch_amount",
                                    "purch_currency",
                                    "purch_note",
                                    "purch_frame",
                                    "purch_code",
                                    "purch_ques",
                                    "purch_loc",
                                    "purch_loc_note",
                                ),
                            },
                            "cost": {
                                "postprocess": lambda x, _: strip_key_prefix("cost_", x),
                                "properties": (
                                    "cost_code",
                                    "cost_translation",
                                    "cost_currency",
                                    "cost_frame",
                                    "cost_description",
                                    "cost_number",
                                ),
                            },
                            "present_location": {
                                "postprocess": lambda x, _: strip_key_prefix("present_loc_", x),
                                "properties": (
                                    "present_loc_geog",
                                    "present_loc_inst",
                                    "present_loc_acc",
                                    "present_loc_note",
                                    "present_loc_ulan_id",
                                ),
                            },
                            "object": {
                                "properties": (
                                    "title",
                                    "description",
                                    "subject",
                                    "genre",
                                    "object_type",
                                    "materials",
                                    "dimensions",
                                    "working_note",
                                    "verbatim_notes",
                                    "editor_notes",
                                    "no_name_notes",
                                    "rosetta_handle",
                                    "sale_location",
                                    "present_location",
                                )
                            },
                        },
                    }
                ],
            ),
            RecordCounter(name="records", verbose=self.debug),
            _input=records.output,
        )

        books = self.add_books_chain(graph, sales_records)
        pages = self.add_pages_chain(graph, books)
        rows = self.add_rows_chain(graph, pages)
        objects = self.add_objects_chain(graph, rows)

        return objects

    def add_transaction_chains(self, graph, tx, services, serialize=True):
        sale = graph.add_chain(ModelSale(helper=self.helper), _input=tx.output)

        people = graph.add_chain(ExtractKeyedValues(key="_people"), _input=sale.output)

        if serialize:
            self.add_person_or_group_chain(graph, people)

    def add_objects_chain(self, graph, rows, serialize=True):
        objects = graph.add_chain(
            PopulateGoupilObject(helper=self.helper), AddArtists(helper=self.helper), _input=rows.output
        )

        odata = graph.add_chain(ExtractKeyedValue(key="_object"), _input=objects.output)
        artists = graph.add_chain(ExtractKeyedValues(key="_artists"), _input=objects.output)

        if serialize:
            self.add_person_or_group_chain(graph, odata, key="_organizations")  # organizations are groups too!
            self.add_person_or_group_chain(graph, artists)
            self.add_places_chain(graph, odata, key="_locations")

        return objects

    def add_books_chain(self, graph, sales_records, serialize=True):
        books = graph.add_chain(
            # add_book,
            AddBooks(static_instances=self.static_instances, helper=self.helper),
            _input=sales_records.output,
        )
        # phys = graph.add_chain(ExtractKeyedValue(key="_physical_book"), _input=books.output)

        textual_works = graph.add_chain(ExtractKeyedValues(key="_book_records"), _input=books.output)

        if serialize:
            # self.add_serialization_chain(graph, act.output, model=self.models['ProvenanceEntry'])
            # self.add_serialization_chain(graph, phys.output, model=self.models["HumanMadeObject"])
            self.add_serialization_chain(graph, textual_works.output, model=self.models["LinguisticObject"])

        return books

    def add_pages_chain(self, graph, books, serialize=True):
        pages = graph.add_chain(
            AddPages(static_instances=self.static_instances, helper=self.helper), _input=books.output
        )

        textual_works = graph.add_chain(ExtractKeyedValues(key="_text_pages"), _input=pages.output)

        if serialize:
            self.add_serialization_chain(graph, textual_works.output, model=self.models["LinguisticObject"])

        return pages

    def add_rows_chain(self, graph, pages, serialize=True):
        rows = graph.add_chain(AddRows(static_instances=self.static_instances, helper=self.helper), _input=pages.output)

        textual_works = graph.add_chain(ExtractKeyedValues(key="_text_rows"), _input=rows.output)

        if serialize:
            self.add_serialization_chain(graph, textual_works.output, model=self.models["LinguisticObject"])

        return rows

    def _construct_graph(self, services=None):
        """
        Construct bonobo.Graph object(s) for the entire pipeline.
        """
        g = bonobo.Graph()

        contents_records = g.add_chain(
            MatchingFiles(path="/", pattern=self.files_pattern, fs="fs.data.goupil"),
            CurriedCSVReader(fs="fs.data.goupil", limit=self.limit, field_names=self.headers),
        )

        sales = self.add_sales_chain(g, contents_records, services, serialize=True)
        self.add_transaction_chains(g, sales, services)

        self.graph = g
        return sales

    def get_graph(self, **kwargs):
        """Return a single bonobo.Graph object for the entire pipeline."""
        if not self.graph:
            self._construct_graph(**kwargs)

        return self.graph

    def run(self, services=None, **options):
        """Run the Goupil bonobo pipeline"""
        if self.verbose:
            print(f"- Limiting to {self.limit} records per file", file=sys.stderr)

        if not services:
            services = self.get_services(**options)

        if self.verbose:
            print("Running graph...", file=sys.stderr)

        graph = self.get_graph(services=services, **options)
        self.run_graph(graph, services=services)

        if self.verbose:
            print("Serializing static instances...", file=sys.stderr)

        for model, instances in self.static_instances.used_instances().items():
            g = bonobo.Graph()
            nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
            values = instances.values()
            source = g.add_chain(GraphListSource(values))
            self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
            self.run_graph(g, services={})


class GoupilFilePipeline(GoupilPipeline):
    """
    Goupil pipeline with serialization to files based on Arches model and resource UUID.

    If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
    serialization will be compact.
    """

    def __init__(self, input_path, data, **kwargs):
        super().__init__(input_path, data, **kwargs)
        self.writers = []
        self.output_path = kwargs.get("output_path")

    def serializer_nodes_for_model(self, *args, model=None, use_memory_writer=True, **kwargs):
        nodes = []
        print(self.output_path)
        if self.debug:
            if use_memory_writer:
                w = MergingMemoryWriter(
                    directory=self.output_path,
                    partition_directories=True,
                    compact=False,
                    model=model,
                )
            else:
                w = MergingFileWriter(
                    directory=self.output_path,
                    partition_directories=True,
                    compact=False,
                    model=model,
                )
            nodes.append(w)
        else:
            if use_memory_writer:
                w = MergingMemoryWriter(
                    directory=self.output_path,
                    partition_directories=True,
                    compact=True,
                    model=model,
                )
            else:
                w = MergingFileWriter(
                    directory=self.output_path,
                    partition_directories=True,
                    compact=True,
                    model=model,
                )
            nodes.append(w)
        self.writers += nodes
        return nodes

    def run(self, **options):
        """Run the Goupil bonobo pipeline."""
        start = timeit.default_timer()
        services = self.get_services(**options)
        super().run(services=services, **options)
        print(f"Pipeline runtime: {timeit.default_timer() - start}", file=sys.stderr)

        count = len(self.writers)
        for seq_no, w in enumerate(self.writers):
            print("[%d/%d] writers being flushed" % (seq_no + 1, count))
            if isinstance(w, MergingMemoryWriter):
                w.flush()

        print("====================================================")
        print("Total runtime: ", timeit.default_timer() - start)
