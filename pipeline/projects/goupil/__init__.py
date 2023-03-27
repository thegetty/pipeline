import csv
import pprint
import sys
import timeit
import warnings
from collections import defaultdict
from contextlib import suppress
from fractions import Fraction

import bonobo
from bonobo.config import Configurable, Option, Service
from cromulent import model, vocab
from cromulent.extract import extract_monetary_amount

import settings
from pipeline.io.csv import CurriedCSVReader
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
from pipeline.linkedart import (
    MakeLinkedArtHumanMadeObject,
    MakeLinkedArtLinguisticObject,
    MakeLinkedArtOrganization,
    MakeLinkedArtPerson,
    MakeLinkedArtRecord,
    PopulateObject,
    add_crom_data,
    get_crom_object,
    get_crom_objects,
    make_la_place,
)
from pipeline.nodes.basic import KeyManagement, RecordCounter
from pipeline.projects import PersonIdentity, PipelineBase
from pipeline.projects.knoedler import SharedUtilityHelper, TransactionHandler
from pipeline.provenance import ProvenanceBase
from pipeline.util import (
    CaseFoldingSet,
    ExtractKeyedValue,
    ExtractKeyedValues,
    GraphListSource,
    MatchingFiles,
    filter_empty_person,
    implode_date,
    strip_key_prefix,
    truncate_with_ellipsis,
)
from pipeline.util.cleaners import parse_location_name


def filter_empty_book(data: dict, _):
    return data if data.get("stock_book_no") else None


def make_place_with_cities_db(data: dict, parent: str, services: dict, base_uri, sales_records: list = []):

    cities_db = services["cities_auth_db"]
    loc_verbatim = data.get("location")

    lower_geography = None
    higher_geography = None

    o_lower = None
    o_higher = None

    if loc_verbatim in cities_db:
        place = cities_db[loc_verbatim]
        auth = place["authority"]
        name = place["name"]
        type = place["type"]

        higher_geography = parse_location_name(auth, uri_base=base_uri)
        make_la_place(higher_geography, base_uri=base_uri)
        o_higher = get_crom_object(higher_geography)

        preferred_name = vocab.PrimaryName(ident="", content=name)
        alternative_name = vocab.AlternateName(ident="", content=loc_verbatim)

        # If the start of the authority column, matches the location name,
        # then we have something like `Baltimore, MD, USA`
        # and we don't need to create another instance for the city.
        # If not, then the case is that we have have a case like `96 boulevard Haussmann, Paris, France`
        # and we need to model the street address as well.
        if not auth.startswith(name):
            lower_geography = make_la_place(
                {
                    "name": name,
                    "type": type,
                    "part_of": higher_geography,
                    "identifiers": [preferred_name, alternative_name],
                },
                base_uri=base_uri,
            )
            o_lower = get_crom_object(lower_geography)
            make_la_place(higher_geography, base_uri=base_uri)
        else:
            o_higher.identified_by = alternative_name
    elif loc_verbatim:
        higher_geography = parse_location_name(loc_verbatim, base_uri=base_uri)
        if len(higher_geography.keys()) == 1 and "name" in higher_geography:
            # if nothing but a dictionary with a name is returned,
            # ignore it to avoiding modelling places heuristically
            pass
        else:
            make_la_place(higher_geography, base_uri=base_uri)
            o_higher = get_crom_object(higher_geography)

    for sale_record in sales_records:
        if o_lower:
            o_lower.referred_to_by = sale_record
        if o_higher:
            o_higher.referred_to_by = sale_record

    if lower_geography:
        parent["_locations"].append(lower_geography)
    elif higher_geography:
        parent["_locations"].append(higher_geography)

    if lower_geography and higher_geography:
        return lower_geography
    elif higher_geography:
        return higher_geography
    else:
        return None


def add_crom_price(data, parent, services, add_citations=False):
    """
    Add modeling data for `MonetaryAmount` based on properties of the supplied `data` dict.
    """
    currencies = services["currencies"]
    amt = data.get("amount", "")
    if "[" in amt:
        data["amount"] = amt.replace("[", "").replace("]", "")
    amnt = extract_monetary_amount(
        data, currency_mapping=currencies, add_citations=add_citations, truncate_label_digits=2
    )
    if amnt:
        for key in ("code", "frame", "uncertain"):
            if data.get(key):
                amnt.referred_to_by = vocab.Note(ident="", content=data[key])
    if amnt:
        add_crom_data(data=data, what=amnt)
    return data


class GoupilPersonIdentity(PersonIdentity):
    pass


def record_id(data):
    no = data["stock_book_no"]
    gno = data["stock_book_gno"]
    page = data["page_number"]
    row = data["row_number"]

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

    def model_object_artists_authority(self, artists: dict):

        for seq_no, a_data in enumerate(artists):
            auth_name = a_data.get("auth_name")
            ulan = a_data.get("ulan_id")
            nationality = a_data.get("nationality")
            places = []
            if a_data.get("location"):
                places.append(a_data.get("location"))

            if a_data.get("auth_location"):
                places.append(a_data.get("auth_location"))

            a_data.update(
                {
                    "ulan": ulan,
                    "label": auth_name,
                    # "places": places,
                }
            )


class AddArtists(ProvenanceBase, GoupilProvenance):
    helper = Option(required=True)
    make_la_person = Service("make_la_person")
    attribution_modifiers = Service("attribution_modifiers")
    attribution_group_types = Service("attribution_group_types")
    attribution_group_names = Service("attribution_group_names")

    def modifiers(self, a: dict):
        mod = a.get("attrib_mod_auth", "")
        if not mod:
            mod = a.get("attrib_mod", "")
        # Matt:  as per George, semantics for 'or' are different in buyer/seller field than in artwork production role. The first does not to my knowledge exist in Goupil.
        # basically treat or as attributed to!
        mod = mod.replace("or", "attributed to")
        mods = CaseFoldingSet({m.strip() for m in mod.split(";")} - {""})
        return mods

    def add_properties(self, data: dict, a: dict):
        a.setdefault("referred_to_by", [])
        a.update(
            {
                "pi_record_no": data["pi_record_no"],
                "goupil_object_id": data["book_record"]["goupil_object_id"],
                "modifiers": self.modifiers(a),
            }
        )

        if self.helper.person_identity.acceptable_person_auth_name(a.get("auth_name")):
            a.setdefault("label", a.get("auth_name"))
        a.setdefault("label", a.get("name"))

    def __call__(
        self, data: dict, *, make_la_person, attribution_modifiers, attribution_group_types, attribution_group_names
    ):
        EDIT_BY = attribution_modifiers["edit by"]
        hmo = get_crom_object(data["_object"])
        self.model_object_artists_authority(data.get("_artists", []))

        sales_records = get_crom_objects(data["_records"])

        for seq_no, artist in enumerate(data.get("_artists", [])):
            mods = self.modifiers(artist)
            if not mods:
                mods = artist.get("attrib_mod", "")
            if EDIT_BY.intersects(mods):
                self.add_properties(data, artist)
                a_data = self.model_person_or_group(
                    data,
                    artist,
                    attribution_group_types,
                    attribution_group_names,
                    seq_no=seq_no,
                    role="Artist",
                    sales_record=sales_records,
                )
                artist_label = artist["label"]
                person = get_crom_object(a_data)

                mod_uri = self.helper.make_shared_uri((hmo.id, "-Modification-By", artist_label))
                modification = model.Modification(ident=mod_uri, label=f'Modification Event for {artist["label"]}')
                modification.carried_out_by = person
                for mod in mods:
                    mod_name = model.Name(ident="", label=f"Modification sub-event for {artist_label}", content=mod)
                    mod_name.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300404670", label="Name")
                    # for record in data.get("_records"):
                    #     mod_name.referred_to_by = get_crom_object(record)
                    modification.identified_by = mod_name
                hmo.modified_by = modification

        self.model_artists_with_modifers(
            data, hmo, attribution_modifiers, attribution_group_types, attribution_group_names
        )

        return data


class GoupilUtilityHelper(SharedUtilityHelper):
    """
    Project-specific code for accessing and interpreting goupil data.
    """

    def __init__(self, project_name, static_instances=None):
        super().__init__(project_name)
        self.person_identity = GoupilPersonIdentity(
            make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri
        )
        self.uid_tag_prefix = self.proj_prefix
        self.csv_source_columns = ["pi_record_no"]

    def stock_number_identifier(self, data, date):
        return super().stock_number_identifier(data, date, stock_data_key="pi_record_no")

    def add_person(self, data, record: None, relative_id, **kwargs):
        self.person_identity.add_uri(data, record_id=relative_id)
        person = super().add_person(data, record=record, relative_id=relative_id, **kwargs)
        if data.get("auth_name"):
            for identifier in person.identified_by:
                if isinstance(identifier, vocab.PrimaryName):
                    identifier.referred_to_by = []

        return person

    def add_group(self, data, record: None, **kwargs):
        group = super().add_group(data, record=record, **kwargs)
        if data.get("auth_name"):
            for identifier in group.identified_by:
                if isinstance(identifier, vocab.PrimaryName):
                    identifier.referred_to_by = []
        return group

    def add_group_or_person(self, p_data, relative_id, people_groups, data):
        auth_name = p_data.get("auth_name", "")
        if people_groups and not auth_name in [x[2] for x in people_groups["group_keys"]]:
            person = self.add_person(p_data, record=get_crom_objects(data["_records"]), relative_id=relative_id)
            add_crom_data(p_data, person)
            data["_people"].append(p_data)
            return person
        else:
            group = self.add_group(p_data, record=get_crom_objects(data["_records"]))
            add_crom_data(p_data, group)
            data["_organizations"].append(p_data)
            return group

    def title_value(self, title):
        if not isinstance(title, str):
            return
        else:
            return title

    def add_title_reference(self, data, title):
        """
        If the title matches the pattern indicating it was added by an editor and has
        and associated source reference, return a `model.LinguisticObject` for that
        reference.

        If the reference can be modeled as a hierarchy of folios and books, that data
        is added to the arrays in the `_physical_objects` and `_linguistic_objects` keys
        of the `data` dict parameter.
        """
        if not isinstance(title, str):
            return None
        return None

    def transaction_key_for_record(
        self, data, incoming=False, purchase_share_key="purchase_knoedler_share", sale_share_key="sale_knoedler_share"
    ):
        """
        Return a URI representing the prov entry which the object (identified by the
        supplied data) is a part of. This may identify just the object being bought or
        sold or, in the case of multiple objects being bought for a single price, a
        single prov entry that encompasses multiple object acquisitions.
        """
        records = data["_records"]
        book_id = page_id = row_id = ""

        for rec in records:
            # TODO: revisit this when the multiple inventorying events thing is resolved
            book_id += rec["stock_book_no"]
            page_id += rec["page_number"]
            row_id += rec["row_number"]

        dir = "In" if incoming else "Out"

        price = data.get(purchase_share_key) if incoming else data.get(sale_share_key)
        if price:
            n = price.get("note")
            if n and n.startswith("for numbers "):
                return ("TX-MULTI", dir, n[12:])
        return ("TX", dir, book_id, page_id, row_id)


class PopulateGoupilObject(Configurable, PopulateObject):
    helper = Option(required=True)
    make_la_org = Service("make_la_or")
    vocab_type_map = Service("vocab_type_map")
    subject_genre = Service("subject_genre")
    cities_auth_db = Service("cities_auth_db")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uid_tag_prefix = self.helper.proj_prefix

    def __call__(self, data: dict, *, vocab_type_map, make_la_org, subject_genre, cities_auth_db):
        sales_records = get_crom_objects(data["_records"])

        assert "_physical_objects" not in data
        data.setdefault("_physical_objects", [])

        assert "_linguistic_objects" not in data
        data.setdefault("_linguistic_objects", [])

        assert "_prov_entries" not in data
        data.setdefault("_prov_entries", [])

        assert "_people" not in data
        data.setdefault("_people", [])

        assert "_locations" not in data
        data.setdefault("_locations", [])

        odata = data["book_record"]

        # split the title and reference in a value such as 「"Collecting her strength" title info from Sales Book 3, 1874-1879, f.252」
        label = self.helper.title_value(odata["title"])
        title_ref = self.helper.add_title_reference(data, odata["title"])

        typestring = odata.get("object_type", "")
        identifiers = []

        title_refs = [sales_records]

        if title_ref:
            title_refs.append(title_ref)
        title = [label, {"referred_to_by": sales_records}]
        data["_object"] = {
            "title": title,
            "identifiers": identifiers,
            "referred_to_by": sales_records,
            "_records": data["_records"],
            "_locations": [],
            "_organizations": [],
            "_records": data["_records"],
        }
        self.helper.copy_source_information(data["_object"], data)
        data["_object"].update(
            {
                k: v
                for k, v in odata.items()
                if k
                in (
                    "materials",
                    "dimensions",
                    "goupil_object_id",
                    "present_location",
                    "object_sale_location",
                    "subject",
                    "genre",
                )
            }
        )

        try:
            goupil_id = odata["goupil_object_id"]
            uri_key = ("Object", goupil_id)
            identifiers.append(self.helper.goupil_pscp_number_id(goupil_id, vocab.StarNumber))
        except Exception as e:
            # warnings.warn(f"*** Object has no goupil object id: {pprint.pformat(data)}")
            uri_key = ("Object", "Internal", data["pi_record_no"])

        for row in data["_records"]:
            try:
                stock_nook_gno = row["stock_book_gno"]
                identifiers.append(self.helper.goupil_number_id(stock_nook_gno, vocab.StockNumber))
            except:
                warnings.warn(f"*** Object has no gno identifier: {pprint.pformat(data)}")

        uri = self.helper.make_object_uri(data["pi_record_no"], *uri_key)
        data["_object"]["uri"] = uri
        data["_object"]["uri_key"] = uri_key

        if typestring in vocab_type_map:
            clsname = vocab_type_map.get(typestring, None)
            otype = getattr(vocab, clsname)
            data["_object"]["object_type"] = otype
        else:
            data["_object"]["object_type"] = model.HumanMadeObject

        make_la_object = MakeLinkedArtHumanMadeObject()
        make_la_object(data["_object"])

        self._populate_object_present_location(data["_object"])
        self._populate_object_visual_item(data["_object"], label, subject_genre)
        self.populate_object_statements(data["_object"], default_unit="inches", strip_comments=True)
        data["_physical_objects"].append(data["_object"])

        hmo = get_crom_object(data["_object"])
        for _records in sales_records:
            _records.about = hmo
        return data

    def _populate_object_present_location(self, data: dict):
        sales_records = get_crom_objects(data["_records"])
        hmo = get_crom_object(data)

        present_location = data.get("present_location", {})
        present_location_verbatim = present_location.get("location")
        note = present_location.get("note")

        if present_location_verbatim:
            current = make_place_with_cities_db(
                present_location, data, services=self.helper.services, base_uri=self.uid_tag_prefix
            )
            curr_place = get_crom_object(current)
            inst = present_location.get("inst")
            if inst:
                owner_data = {
                    "label": f"{inst} ({present_location_verbatim})",
                    "identifiers": [vocab.PrimaryName(ident="", content=inst)],
                }
                ulan = None
                with suppress(ValueError, TypeError):
                    ulan = int(present_location.get("ulan_id"))
                if ulan:
                    owner_data["ulan"] = ulan
                    owner_data["uri"] = self.helper.make_proj_uri("ORG", "ULAN", ulan)
                else:
                    owner_data["uri"] = self.helper.make_proj_uri(
                        "ORG", "NAME", inst, "PLACE", present_location_verbatim
                    )
            else:
                warnings.warn(
                    f"*** Object present location data has a location, but not an institution: {pprint.pformat(data)}"
                )
                owner_data = {
                    "label": "(Anonymous organization)",
                    "uri": self.helper.make_proj_uri("ORG", "CURR-OWN", "UNKNOWN-ORG"),
                    # "referred_to_by": [sales_record],
                }

            owner_data["referred_to_by"] = sales_records
            if current:
                curr_place = get_crom_object(current)
                hmo.current_location = curr_place

            owner = None
            if owner_data:
                make_la_org = MakeLinkedArtOrganization()
                owner_data = make_la_org(owner_data)
                owner = get_crom_object(owner_data)
                hmo.current_owner = owner
                owner.residence = curr_place

            if note:
                owner_data["note"] = note
                desc = vocab.Description(ident="", content=note)
                if owner:
                    assignment = model.AttributeAssignment(ident="")
                    assignment.carried_out_by = owner
                    desc.assigned_by = assignment
                hmo.referred_to_by = desc

            acc = present_location.get("acc")
            if acc:
                acc_number = vocab.AccessionNumber(ident="", content=acc)
                hmo.identified_by = acc_number
                assignment = model.AttributeAssignment(ident="")
                if owner:
                    assignment.carried_out_by = owner
                acc_number.assigned_by = assignment

            data["_organizations"].append(owner_data)
            data["_final_org"] = owner_data

    def _populate_object_visual_item(self, data: dict, title, subject_genre):
        sales_records = get_crom_objects(data["_records"])
        hmo = get_crom_object(data)
        title = truncate_with_ellipsis(title, 100) or title

        # The visual item URI is just the object URI with a suffix. When URIs are
        # reconciled during prev/post sale rewriting, this will allow us to also reconcile
        # the URIs for the visual items (of which there should only be one per object)
        vi_uri = hmo.id + "-VisItem"
        vi = model.VisualItem(
            ident=vi_uri,
        )
        vi._label = f"Visual Work of “{title}”"
        vidata = {
            "uri": vi_uri,
            "referred_to_by": sales_records,
            "identifiers": [],
        }
        if title:
            vidata["label"] = f"Visual Work of “{title}”"
            t = vocab.Name(ident="", content=title)
            t.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300417193", label="Title")
            for sale_record in sales_records:
                t.referred_to_by = sale_record
            vidata["identifiers"].append(t)

        # TODO: refactor when AR-164 is merged
        objgenre = data.get("genre")
        objsubject = data.get("subject")

        if objgenre and objsubject:
            key = ", ".join((objsubject.strip(), objgenre.strip())).lower()
        elif objgenre:
            key = objgenre.strip().lower()
        elif objsubject:
            key = objsubject.strip().lower()
        else:
            key = None

        for prop, mappings in subject_genre.items():
            if key in mappings:
                for label, types in mappings[key].items():
                    type = model.Type(ident=types["type"], label=label)

                    if "metatype" in types:
                        metatype = model.Type(ident=types["metatype"], label="object/work type")
                        setattr(type, prop, metatype)

                    setattr(vi, prop, type)
        data["_visual_item"] = add_crom_data(data=vidata, what=vi)
        hmo.shows = vi


class AddBooks(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_lo = Service("make_la_lo")
    make_la_hmo = Service("make_la_hmo")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo):
        books = data.get("_book_records", [])

        assert "_physical_books" not in data
        data.setdefault("_physical_books", [])

        physical_objects = data.get("_physical_books")

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

            physical_book = {
                "uri": self.helper.make_proj_uri("Book", book_id),
                "object_type": vocab.Book,
                "label": (label, vocab.instances["english"]),
                "identifiers": [
                    self.helper.goupil_number_id(book_id, id_class=vocab.BookNumber),
                ],
                "carries": [book],
            }

            make_la_lo(book)
            make_la_hmo(physical_book)
            o_book = get_crom_object(book)
            p_book = get_crom_object(physical_book)
            self.add_goupil_creation_data(book)
            b_data.update(book)
            physical_objects.append(physical_book)

        return data


class AddPages(Configurable, GoupilProvenance):
    helper = Option(required=True)
    make_la_lo = Service("make_la_lo")
    make_la_hmo = Service("make_la_hmo")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo):
        books = data.get("_book_records", [])
        physical_books = data.get("_physical_books", [])
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
                "identifiers": [
                    self.helper.goupil_number_id(page, id_class=vocab.PageNumber),
                ],
            }

            page.update(
                {
                    k: v
                    for k, v in b_data.items()
                    if k in ("stock_book_no", "stock_book_gno", "page_number", "row_number")
                }
            )
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
    transaction_classification = Service("transaction_classification")
    static_instances = Option(default="static_instances")

    def __call__(self, data: dict, make_la_lo, make_la_hmo, transaction_classification):
        pages = data.get("_text_pages", [])
        data.setdefault("_records", [])

        notes = []
        for k in ("working_note", "verbatim_notes", "editor notes", "no_name_notes"):
            if data["book_record"].get(k):
                notes.append(vocab.Note(ident="", content=data["book_record"][k]))

        if data["book_record"].get("rosetta_handle"):
            page = vocab.DigitalImage(
                ident=data["book_record"]["rosetta_handle"], label=data["book_record"]["rosetta_handle"]
            )
            page._validate_range = False
            page.access_point = [vocab.DigitalObject(ident=data["book_record"]["rosetta_handle"])]
            notes.append(page)

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
                        row,
                        id_class=vocab.RowNumber,
                        assignment_label=f"Entry Number Attribution by {self.static_instances.get_instance('Group', 'goupil')._label}",
                    ),
                    self.helper.goupil_gpi_number_id(data["pi_record_no"], vocab.StarNumber),
                ],
                "referred_to_by": notes,
            }
            row.update(
                {
                    k: v
                    for k, v in p_data.items()
                    if k in ("stock_book_no", "stock_book_gno", "page_number", "row_number")
                }
            )
            make_la_lo(row)

            o_page = get_crom_object(p_data)
            o_row = get_crom_object(row)
            o_row.part_of = o_page

            transaction = data["book_record"]["transaction"]
            tx_cl = transaction_classification.get(transaction)

            if tx_cl:
                label = tx_cl.get("label")
                url = tx_cl.get("url")
                o_row.about = model.Type(ident=url, label=label)
            else:
                warnings.warn(f"*** No classification found for transaction type: {transaction!r}")

            data["_records"].append(row)
            self.add_goupil_creation_data(row)
            # TODO Uncomment the following lines in order to have date information for the text records
            # creation = self.add_goupil_creation_data(row)
            # date = implode_date(data['entry_date'])
            # if date:
            #     begin_date = implode_date(data['entry_date'], clamp='begin')
            #     end_date = implode_date(data['entry_date'], clamp='end')
            #     bounds = [begin_date, end_date]
            #     ts = timespan_from_outer_bounds(*bounds, inclusive=True)
            #     ts.identified_by = model.Name(ident='', content=date)
            #     creation.timespan = ts

        return data


class TransactionSwitch(Configurable):
    """
    @see pipeline/projects/knoedler/__init__.py#TransactionSwitch
    """

    transaction_classification = Service("transaction_classification")

    def __call__(self, data: dict, transaction_classification):
        rec = data["book_record"]
        transaction = rec["transaction"]
        tx = transaction_classification.get(transaction)
        if tx:
            rec["transaction_verbatim"] = transaction
            rec["transaction"] = tx["label"]
            yield {tx["label"]: data}
        else:
            warnings.warn(f"TODO: handle transaction type `{transaction}`")


class GoupilTransactionHandler(TransactionHandler):
    helper = Option(required=True)
    cities_auth_db = Service("cities_auth_db")

    def modifiers(self, a: dict, key: str):
        return {a.get(key, "").split(" ")[0]}

    def model_seller_buyer_authority(self, p_data: dict):
        auth_name = p_data.get("auth_name")
        ulan = p_data.get("ulan_id")

        p_data.update(
            {
                "ulan": ulan,
                "label": auth_name,
            }
        )

    def person_sojourn(self, p_data: dict, sojourn, sales_records):
        act = model.Activity(ident=self.helper.make_proj_uri("ACT", p_data["label"]), label="Sojourn activity")
        act.classified_as = model.Type(
            ident="http://vocab.getty.edu/aat/300393212", label="establishment (action or condition)"
        )
        act.took_place_at = sojourn
        person = get_crom_object(p_data)
        person.carried_out = act
        for record in sales_records:
            act.referred_to_by = record

    def model_prev_post_owners(self, data, owner: str, role, people_groups):
        splitOwners = [{k: v if k != "name" else x for k, v in owner.items()} for x in owner["name"].split("; ")]
        for i, p in enumerate(splitOwners):
            person_dict = self.helper.copy_source_information(p, data)
            person = self.helper.add_group_or_person(
                person_dict, relative_id=f"{role}_{i+1}", people_groups=people_groups, data=data
            )
            data["_people"].append(person_dict)

    def _apprasing_assignment(self, data):
        odata = data["_object"]
        date = implode_date(data["entry_date"])

        hmo = get_crom_object(odata)
        sn_ident = self.helper.stock_number_identifier(odata, date)

        sellers = data["purchase_seller"]
        price_info = data.get("cost")
        if price_info and not len(sellers):
            amnt = get_crom_object(price_info)
            assignment = vocab.AppraisingAssignment(ident="", label=f"Evaluated worth of {sn_ident}")
            assignment.carried_out_by = self.helper.static_instances.get_instance("Group", "goupil")
            assignment.assigned_property = "dimension"
            if amnt:
                assignment.assigned = amnt
                amnt.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300412096", label="Valuation")
            assignment.assigned_to = hmo
            return assignment
        return None

    def _new_inventorying(self, data):
        odata = data["_object"]
        date = implode_date(data["entry_date"])

        hmo = get_crom_object(odata)
        sn_ident = self.helper.stock_number_identifier(odata, date)
        inv_label = f"Goupil Inventorying of {sn_ident}"

        records = data["_records"]
        book_id = page_id = row_id = ""

        for rec in records:
            # TODO: revisit this when the multiple inventorying events thing is resolved
            book_id += rec["stock_book_no"]
            page_id += rec["page_number"]
            row_id += rec["row_number"]

        inv_uri = self.helper.make_proj_uri("INV", book_id, page_id, row_id)
        inv = vocab.Inventorying(ident=inv_uri, label=inv_label)
        inv.identified_by = model.Name(ident="", content=inv_label)
        inv.encountered = hmo
        inv.carried_out_by = self.helper.static_instances.get_instance("Group", "goupil")
        self.set_date(inv, data, "entry_date")

        return inv

    def _maintainance_activity(self, data, tx):
        odata = data["_object"]
        date = implode_date(data["entry_date"])

        hmo = get_crom_object(odata)
        sn_ident = self.helper.stock_number_identifier(odata, date)

        # TODO to be clarified if this should be the purchase or cost field, or both
        price_info = data.get("cost")
        if price_info:
            # this inventorying has a "Maintainance" expense
            amnt = get_crom_object(price_info)
            maintainance = vocab.Activity(ident="", label=f"Maintainance of {sn_ident}")
            maintainance.carried_out_by = self.helper.static_instances.get_instance("Group", "goupil")
            tx_uri = tx.id
            payment_id = tx_uri + "-Payment"
            paym = model.Payment(ident=payment_id, label=f"Payment for maintainance of {sn_ident}")
            paym.paid_amount = amnt
            tx.part = paym
            amnt.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300412096", label="Maintainance")
            # maintainance.assigned_to = hmo
            return maintainance
        return None

    def _empty_tx(self, data, incoming=False, purpose=None):
        tx_uri = self.helper.transaction_uri_for_record(data, incoming)
        tx_type = data.get("book_record", {}).get("transaction", "Sold")
        if purpose == "returning":
            tx = vocab.make_multitype_obj(vocab.SaleAsReturn, vocab.ProvenanceEntry, ident=tx_uri)
        else:
            tx = vocab.ProvenanceEntry(ident=tx_uri)
        sales_records = get_crom_objects(data["_records"])

        for sales_record in sales_records:
            tx.referred_to_by = sales_record

        return tx

    def _add_prov_entry_acquisition(
        self, data: dict, tx, from_people, from_agents, to_people, to_agents, date, incoming, purpose=None
    ):

        hmo = get_crom_object(data["_object"])
        sn_ident = self.helper.stock_number_identifier(data["_object"], date)
        sale_location = data["book_record"].get("object_sale_location", {})
        sale_location_verbatim = sale_location.get("location")

        place = make_place_with_cities_db(
            sale_location,
            data,
            services=self.helper.services,
            base_uri=self.helper.uid_tag_prefix,
        )
        o_place = get_crom_object(place)

        dir = "In" if incoming else "Out"
        if purpose == "returning":
            dir_label = "Goupil return"
        else:
            dir_label = "Goupil Purchase" if incoming else "Goupil Sale"
        # We have a different way of creating the uri, becuases there are multiple rows in each entry and we don't want to create multiple Acquisition events
        tx_uri = tx.id
        acq_id = tx_uri + "-Acquisition"
        acq = model.Acquisition(ident=acq_id)

        sn_ident = self.helper.stock_number_identifier(data["_object"], date)
        name = f"{dir_label} of {sn_ident}"
        tx.identified_by = model.Name(ident="", content=name)
        tx._label = name
        acq._label = name
        acq.identified_by = model.Name(ident="", content=name)
        acq.transferred_title_of = hmo

        if place:
            acq.took_place_at = o_place
        elif sale_location_verbatim:
            acq.referred_to_by = vocab.Note(content=sale_location_verbatim)

        for p in from_people:
            acq.transferred_title_from = p
        for p in from_agents:
            # when an agent is acting on behalf of the seller, model their involvement in a sub-activity
            subacq = model.Activity(ident="", label="Seller's agent's role in acquisition")
            subacq.classified_as = vocab.instances["SellersAgent"]
            subacq.carried_out_by = p
            acq.part = subacq
        for p in to_people:
            acq.transferred_title_to = p
        for p in to_agents:
            # when an agent is acting on behalf of the buyer, model their involvement in a sub-activity
            subacq = model.Activity(ident="", label="Buyer's agent's role in acquisition")
            subacq.classified_as = vocab.instances["BuyersAgent"]
            subacq.carried_out_by = p
            acq.part = subacq

        tx.part = acq

    def _add_prov_entry_payment(
        self,
        data: dict,
        tx,
        goupil_price_part,
        price_info,
        people,
        people_agents,
        shared_people,
        shared_people_agents,
        date,
        incoming,
        people_groups=None,
    ):
        goupil = self.helper.static_instances.get_instance("Group", "goupil")
        goupil_group = [goupil]

        sn_ident = self.helper.stock_number_identifier(data["_object"], date)

        price_data = {}
        if price_info and "currency" in price_info:
            price_data["currency"] = price_info["currency"]

        amnt = get_crom_object(price_info)
        goupil_price_part_amnt = get_crom_object(goupil_price_part)

        parts = [(goupil, goupil_price_part_amnt)]
        if shared_people:
            role = "shared-buyer" if incoming else "shared-seller"
            for i, p in enumerate(shared_people):
                person_dict = self.helper.copy_source_information(p, data)
                person = self.helper.add_group_or_person(
                    person_dict, relative_id=f"{role}_{i+1}", people_groups=people_groups, data=data
                )
                goupil_group.append(person)

        paym = None
        if amnt:
            tx_uri = tx.id
            payment_id = tx_uri + "-Payment"
            paym = model.Payment(ident=payment_id, label=f"Payment for {sn_ident}")
            paym.paid_amount = amnt
            tx.part = paym
            for kp in goupil_group:
                if incoming:
                    paym.paid_from = kp
                else:
                    paym.paid_to = kp
            for p in shared_people_agents:
                # when an agent is acting on behalf of the buyer/seller, model their involvement in a sub-activity
                subpaym_role = "Buyer" if incoming else "Seller"
                subpaym = model.Activity(ident="", label=f"{subpaym_role}'s agent's role in payment")
                subpaym.classified_as = vocab.instances[f"{subpaym_role}sAgent"]
                subpaym.carried_out_by = p
                paym.part = subpaym

            for i, partdata in enumerate(parts):
                person, part_amnt = partdata
                # add the part is there are multiple parts (shared tx), or if
                # this is the single part we know about, but its value is not
                # the same as the whole tx amount
                different_amount = False
                with suppress(AttributeError):
                    if amnt.value != part_amnt.value:
                        different_amount = True
                if len(parts) > 1 or different_amount:
                    shared_payment_id = tx_uri + f"-Payment-{i}-share"
                    shared_paym = model.Payment(
                        ident=shared_payment_id, label=f"{person._label} share of payment for {sn_ident}"
                    )
                    if part_amnt:
                        shared_paym.paid_amount = part_amnt
                    if incoming:
                        shared_paym.paid_from = person
                    else:
                        shared_paym.paid_to = person

                    paym.part = shared_paym

        for person in people:
            if paym:
                if incoming:
                    paym.paid_to = person
                else:
                    paym.paid_from = person
        for p in people_agents:
            # when an agent is acting on behalf of the buyer/seller, model their involvement in a sub-activity
            if paym:
                subpaym_role = "Seller" if incoming else "Buyer"
                subpaym = model.Activity(ident="", label=f"{subpaym_role}'s agent's role in payment")
                subpaym.classified_as = vocab.instances[f"{subpaym_role}sAgent"]
                subpaym.carried_out_by = p
                paym.part = subpaym

    def _add_prov_entry_rights(self, data: dict, tx, shared_people, incoming, people_groups=None):
        goupil = self.helper.static_instances.get_instance("Group", "knoedler")
        sales_records = get_crom_objects(data["_records"])

        hmo = get_crom_object(data["_object"])
        object_label = f"“{hmo._label}”"

        # this is the group of people along with Knoedler that made the purchase/sale (len > 1 when there is shared ownership)
        goupil_group = [goupil]
        if shared_people:
            people = []
            rights = []
            role = "shared-buyer" if incoming else "shared-seller"
            remaining = Fraction(1, 1)
            for i, p in enumerate(shared_people):
                person_dict = self.helper.copy_source_information(p, data)
                person = self.helper.add_group_or_person(
                    person_dict, relative_id=f"{role}_{i+1}", people_groups=people_groups, data=data
                )
                name = p.get("name", p.get("auth_name", "(anonymous)"))
                if p.get("share") == "":
                    p["share"] = "1/1"
                share = p.get("share", "1/1")
                try:
                    share_frac = Fraction(share)
                    remaining -= share_frac

                    right = self.ownership_right(share_frac, person)

                    rights.append(right)
                    people.append(person_dict)
                    goupil_group.append(person)
                except ValueError as e:
                    warnings.warn(f"ValueError while handling shared rights ({e}): {pprint.pformat(p)}")
                    raise

            g_right = self.ownership_right(remaining, goupil)
            rights.insert(0, g_right)

            total_right = vocab.OwnershipRight(ident="", label=f"Total Right of Ownership of {object_label}")
            total_right.applies_to = hmo
            for right in rights:
                total_right.part = right

            racq = model.RightAcquisition(ident="")
            racq.establishes = total_right
            tx.part = racq

            data["_people"].extend(people)

    def _prov_entry(
        self,
        data,
        date_key,
        participants,
        price_info=None,
        goupil_price_part=None,
        shared_people=None,
        incoming=False,
        purpose=None,
        buy_sell_modifiers=None,
        people_groups=None,
    ):
        THROUGH = CaseFoldingSet(buy_sell_modifiers["through"])

        if shared_people is None:
            shared_people = []

        date = implode_date(data[date_key]) if date_key in data else None

        sales_records = get_crom_objects(data["_records"])

        tx = self._empty_tx(data, incoming, purpose=purpose)
        tx_uri = tx.id

        tx_data = add_crom_data(data={"uri": tx_uri}, what=tx)
        if date_key:
            self.set_date(tx, data, date_key)

        role = "seller" if incoming else "buyer"

        people_data = [self.helper.copy_source_information(p, data) for p in participants]

        people = []
        people_agents = []
        for i, p_data in enumerate(people_data):
            mod = self.modifiers(p_data, "auth_mod")
            self.model_seller_buyer_authority(p_data)
            person = self.helper.add_group_or_person(
                p_data, relative_id=f"{role}_{i+1}", people_groups=people_groups, data=data
            )

            place_data = make_place_with_cities_db(
                p_data,
                data,
                services=self.helper.services,
                base_uri=self.helper.uid_tag_prefix,
                sales_records=sales_records,
            )
            place = get_crom_object(place_data)
            if place:
                person.residence = place
            else:
                loc_verbatim = p_data.get("location")
                if loc_verbatim:
                    tx.referred_to_by = vocab.Note(content=loc_verbatim)

            if place and "shared#PLACE" in place.id:
                self.person_sojourn(p_data, place, sales_records)
            if THROUGH.intersects(mod):
                people_agents.append(person)
            else:
                people.append(person)

        goupil_group = [self.helper.static_instances.get_instance("Group", "goupil")]
        goupil_group_agents = []
        if shared_people:
            # these are the people that joined Knoedler in the purchase/sale
            role = "shared-buyer" if incoming else "shared-seller"
            for i, p_data in enumerate(shared_people):
                self.model_seller_buyer_authority(p_data)
                person_dict = self.helper.copy_source_information(p_data, data)
                if not p_data.get("label"):
                    p_data["label"] = p_data.get("name")
                person = self.helper.add_group_or_person(
                    person_dict, relative_id=f"{role}_{i+1}", people_groups=people_groups, data=data
                )

        from_people = []
        from_agents = []
        to_people = []
        to_agents = []
        if incoming:
            from_people = people
            from_agents = people_agents
            to_people = goupil_group
            to_agents = goupil_group_agents
        else:
            from_people = goupil_group
            from_agents = goupil_group_agents
            to_people = people
            to_agents = people_agents

        if incoming:
            self._add_prov_entry_rights(data, tx, shared_people, incoming, people_groups)
        self._add_prov_entry_payment(
            data,
            tx,
            goupil_price_part,
            price_info,
            people,
            people_agents,
            shared_people,
            goupil_group_agents,
            date,
            incoming,
            people_groups,
        )
        self._add_prov_entry_acquisition(
            data, tx, from_people, from_agents, to_people, to_agents, date, incoming, purpose=purpose
        )
        data["_prov_entries"].append(tx_data)
        data["_people"].extend(people_data)
        return tx

    def add_incoming_tx(self, data, buy_sell_modifiers, people_groups=None):
        price_info = data.get("purchase")

        shared_people = data.get("shared_buyer")
        sellers = data["purchase_seller"]

        for p in sellers:
            self.helper.copy_source_information(p, data)
        tx = self._prov_entry(
            data,
            "entry_date",
            sellers,
            price_info,
            shared_people=shared_people,
            incoming=True,
            buy_sell_modifiers=buy_sell_modifiers,
            people_groups=people_groups,
        )
        prev_owners = data.get("prev_own", {})
        if prev_owners:
            self.model_prev_post_owners(data, prev_owners, "prev_own", people_groups)

        return tx

    def add_outgoing_tx(self, data, buy_sell_modifiers, people_groups=None):
        price_info = data.get("sale")
        shared_people = data.get("shared_buyer")
        buyers = data["sale_buyer"]
        for p in buyers:
            self.helper.copy_source_information(p, data)
        tx = self._prov_entry(
            data,
            "sale_date",
            buyers,
            price_info,
            shared_people=shared_people,
            incoming=False,
            buy_sell_modifiers=buy_sell_modifiers,
            people_groups=people_groups,
        )
        post_own = data.get("post_own", {})
        if post_own:
            self.model_prev_post_owners(data, post_own, "post_own", people_groups)

        return tx


class ModelSale(GoupilTransactionHandler):
    """ """

    helper = Option(required=True)
    make_la_person = Service("make_la_person")
    buy_sell_modifiers = Service("buy_sell_modifiers")
    people_groups = Service("people_groups")
    cities_auth_db = Service("cities_auth_db")

    def __call__(
        self,
        data: dict,
        make_la_person,
        people_groups,
        buy_sell_modifiers=None,
        in_tx=None,
        out_tx=None,
        cities_auth_db=None,
    ):
        sellers = data["purchase_seller"]

        if not in_tx:
            if len(sellers):
                in_tx = self.add_incoming_tx(data, buy_sell_modifiers, people_groups)
            # if there are no sellers or there is a maintenance cost create an ivnentorying event
            else:
                inv = self._new_inventorying(data)
                appraisal = self._apprasing_assignment(data)
                inv_label = inv._label
                in_tx = self._empty_tx(data, incoming=True)
                in_tx.part = inv
                if appraisal:
                    in_tx.part = appraisal
                in_tx.identified_by = model.Name(ident="", content=inv_label)
                in_tx._label = inv_label
                in_tx_data = add_crom_data(data={"uri": in_tx.id, "label": inv_label}, what=in_tx)
                data["_prov_entries"].append(in_tx_data)

        if not out_tx:
            out_tx = self.add_outgoing_tx(data, buy_sell_modifiers, people_groups)
        in_tx.ends_before_the_start_of = out_tx
        out_tx.starts_after_the_end_of = in_tx

        yield data


class ModelInventorying(GoupilTransactionHandler):
    helper = Option(required=True)
    make_la_person = Service("make_la_person")
    buy_sell_modifiers = Service("buy_sell_modifiers")
    cities_auth_db = Service("cities_auth_db")

    def __call__(self, data: dict, make_la_person, buy_sell_modifiers, cities_auth_db):

        sellers = data["purchase_seller"]
        if len(sellers) > 0:
            # if there are sellers in this record (and it is "Unsold" by design of the caller),
            # then this is not an actual Inventorying event, and handled in ModelUnsoldPurchases
            return

        inv = self._new_inventorying(data)
        appraisal = self._apprasing_assignment(data)
        inv_label = inv._label

        tx_out = self._empty_tx(data, incoming=False)
        tx_out._label = inv_label
        tx_out.identified_by = model.Name(ident="", content=inv_label)
        self.set_date(tx_out, data, "entry_date")

        tx_out.part = inv

        if appraisal:
            tx_out.part = appraisal

        tx_out_data = add_crom_data(data={"uri": tx_out.id, "label": inv_label}, what=tx_out)

        data["_prov_entries"].append(tx_out_data)

        yield data


class ModelUnsoldPurchases(GoupilTransactionHandler):
    helper = Option(required=True)
    make_la_person = Service("make_la_person")
    buy_sell_modifiers = Service("buy_sell_modifiers")
    people_groups = Service("people_groups")
    cities_auth_db = Service("cities_auth_db")

    def __call__(self, data: dict, make_la_person, buy_sell_modifiers, people_groups, cities_auth_db):
        odata = data["_object"]
        date = implode_date(data["entry_date"])

        sellers = data["purchase_seller"]
        if len(sellers) == 0:
            return

        hmo = get_crom_object(odata)
        object_label = f"“{hmo._label}”"

        sn_ident = self.helper.stock_number_identifier(odata, date)

        in_tx = self.add_incoming_tx(data, buy_sell_modifiers, people_groups)

        yield data


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
            "SaleAsReturn", {"parent": model.Activity, "id": "300445014", "label": "Sale (Return to Original Owner)"}
        )
        vocab.register_vocab_class(
            "AppraisingAssignment", {"parent": model.AttributeAssignment, "id": "300054622", "label": "Appraising"}
        )

        vocab.register_vocab_class(
            "ConstructedTitle", {"parent": model.Name, "id": "300417205", "label": "Constructed Title"}
        )
        vocab.register_vocab_class(
            "BookNumber", {"parent": model.Identifier, "id": "300445021", "label": "Book Numbers"}
        )

        vocab.register_vocab_class(
            "PageNumber", {"parent": model.Identifier, "id": "300445022", "label": "Page Numbers"}
        )

        vocab.register_vocab_class(
            "RowNumber", {"parent": model.Identifier, "id": "300445023", "label": "Entry Number"}
        )

        vocab.register_vocab_class(
            "UncertainMemberClosedGroup",
            {"parent": model.Group, "id": "300448855", "label": "Closed Group Representing an Uncertain Person"},
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
        services["same_objects_map"] = {}
        services["different_objects"] = {}
        # make these case-insensitive by wrapping the value lists in CaseFoldingSet
        for name in ("attribution_modifiers",):
            if name in services:
                services[name] = {k: CaseFoldingSet(v) for k, v in services[name].items()}

        if "attribution_modifiers" in services:
            attribution_modifiers = services["attribution_modifiers"]
            PROBABLY = attribution_modifiers["probably by"]
            POSSIBLY = attribution_modifiers["possibly by"]
            attribution_modifiers["uncertain"] = PROBABLY | POSSIBLY

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
                                    "stock_book_no": "stock_book_no",
                                    "stock_book_gno": "stock_book_gno",
                                    "stock_book_pg": "page_number",
                                    "stock_book_row": "row_number",
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
                            # remove sale from group repeating and move it to group until the price_amount_2 semantic meaning is decided
                            # "sale": {
                            #     "rename_keys": {
                            #         "price_amount": "amount",
                            #         "price_code": "code",
                            #         "price_currency": "currency",
                            #         "price_note": "note",
                            #     },
                            #     "prefixes": ("price_amount", "price_code", "price_currency", "price_note"),
                            # },
                            "purchase_seller": {
                                "postprocess": [
                                    filter_empty_person,
                                    lambda x, _: strip_key_prefix("seller_", x),
                                    lambda x, _: strip_key_prefix("sell_", x),
                                ],
                                "prefixes": (
                                    "seller_name",
                                    "seller_loc",
                                    "sell_auth_name",
                                    "sell_auth_loc",
                                    "sell_auth_mod",
                                    "seller_ulan_id",
                                ),
                            },
                            "shared_buyer": {
                                "rename_keys": {
                                    "joint_own": "name",
                                    "joint_own_sh": "share",
                                    "joint_ulan_id": "ulan_id",
                                },
                                "prefixes": ("joint_own", "joint_own_sh", "joint_ulan_id"),
                            },
                            "sale_buyer": {
                                "rename_keys": {
                                    "buyer_name": "name",
                                    "buyer_loc": "loc",
                                    "buy_auth_name": "auth_name",
                                    "buy_auth_addr": "location",
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
                            "sale": {
                                "rename_keys": {
                                    "price_amount_1": "amount",
                                    "price_code_1": "code",
                                    "price_currency_1": "currency",
                                    "price_note_1": "note",
                                },
                                "postprocess": [
                                    lambda d, p: add_crom_price(d, p, services),
                                    # lambda d, p: add_crom_price(d, p, services)
                                    # TODO handle price code
                                ],
                                "properties": ("price_amount_1", "price_code_1", "price_currency_1", "price_note_1"),
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
                                    # TODO create purchase location place
                                },
                                "postprocess": [
                                    lambda d, p: add_crom_price(d, p, services),
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
                                "postprocess": [
                                    lambda d, p: add_crom_price(d, p, services),
                                ],
                                "rename_keys": {
                                    "cost_code": "code",
                                    "cost_translation": "amount",
                                    "cost_currency": "currency",
                                    "cost_frame": "frame",
                                    "cost_description": "note",
                                    "cost_number": "uncertain",
                                },
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
                                "rename_keys": {"present_loc_geog": "location"},
                            },
                            "object_sale_location": {
                                "properties": ("sale_location",),
                                "rename_keys": {"sale_location": "location"},
                            },
                            "book_record": {
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
                                    "object_sale_location",
                                    "present_location",
                                    "goupil_object_id",
                                    "goupil_event_ord",  # TODO: for future reference only, semantics uknown at this point
                                    "transaction",
                                ),
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

        tx = graph.add_chain(TransactionSwitch(), _input=objects.output)
        return tx

    def add_transaction_chains(self, graph, tx, services, serialize=True):
        inventorying = graph.add_chain(
            ExtractKeyedValue(key="Inventorying"),
            ModelInventorying(helper=self.helper),
            _input=tx.output,
        )

        unsold_purchases = graph.add_chain(
            ExtractKeyedValue(key="Inventorying"),
            ModelUnsoldPurchases(helper=self.helper),
            _input=tx.output,
        )

        sale = graph.add_chain(
            ExtractKeyedValue(key="Purchase"),
            ModelSale(helper=self.helper),
            _input=tx.output,
        )

        # returned = graph.add_chain(
        #     ExtractKeyedValue(key="Returned"),
        #     ModelReturn(helper=self.helper),
        #     _input=tx.output,
        # )

        # destruction = graph.add_chain(
        #     ExtractKeyedValue(key="Destroyed"),
        #     ModelDestruction(helper=self.helper),
        #     _input=tx.output,
        # )

        # theft = graph.add_chain(
        #     ExtractKeyedValue(key="Stolen"),
        #     ModelTheftOrLoss(helper=self.helper),
        #     _input=tx.output,
        # )

        # loss = graph.add_chain(
        #     ExtractKeyedValue(key="Lost"),
        #     ModelTheftOrLoss(helper=self.helper),
        #     _input=tx.output,
        # )

        # activities are specific to the inventorying chain
        activities = graph.add_chain(ExtractKeyedValues(key="_activities"), _input=inventorying.output)
        if serialize:
            self.add_serialization_chain(graph, activities.output, model=self.models["Inventorying"])

        # # people and prov entries can come from any of these chains:
        for branch in (
            sale,
            # destruction,
            # theft,
            # loss,
            inventorying,
            unsold_purchases,
            # returned,
        ):
            prov_entry = graph.add_chain(ExtractKeyedValues(key="_prov_entries"), _input=branch.output)
            people = graph.add_chain(ExtractKeyedValues(key="_people"), _input=branch.output)
            groups = graph.add_chain(ExtractKeyedValues(key="_organizations"), _input=sale.output)
            _ = self.add_places_chain(graph, branch, key="_locations", serialize=serialize, include_self=True)

            if serialize:
                self.add_serialization_chain(graph, prov_entry.output, model=self.models["ProvenanceEntry"])
                self.add_person_or_group_chain(graph, people)
                self.add_person_or_group_chain(graph, groups)

    def add_objects_chain(self, graph, rows, serialize=True):
        objects = graph.add_chain(
            PopulateGoupilObject(helper=self.helper),
            AddArtists(helper=self.helper),
            _input=rows.output,
        )
        people = graph.add_chain(ExtractKeyedValues(key="_people"), _input=objects.output)
        hmos1 = graph.add_chain(ExtractKeyedValues(key="_physical_objects"), _input=objects.output)
        hmos2 = graph.add_chain(ExtractKeyedValues(key="_original_objects"), _input=objects.output)
        texts = graph.add_chain(ExtractKeyedValues(key="_linguistic_objects"), _input=objects.output)
        odata = graph.add_chain(ExtractKeyedValue(key="_object"), _input=objects.output)
        # final_sale = graph.add_chain(ModelFinalSale(helper=self.helper), _input=objects.output)
        # prov_entry = graph.add_chain(ExtractKeyedValues(key="_prov_entries"), _input=final_sale.output)
        # people2 = graph.add_chain(ExtractKeyedValues(key="_people"), _input=final_sale.output)

        artists = graph.add_chain(ExtractKeyedValues(key="_artists"), _input=objects.output)
        groups1 = graph.add_chain(ExtractKeyedValues(key="_organizations"), _input=objects.output)
        groups2 = graph.add_chain(ExtractKeyedValues(key="_organizations"), _input=hmos1.output)
        owners = self.add_person_or_group_chain(graph, hmos1, key="_other_owners", serialize=serialize)

        items = graph.add_chain(
            ExtractKeyedValue(key="_visual_item"),
            MakeLinkedArtRecord(),
            _input=hmos1.output,
        )

        if serialize:
            self.add_serialization_chain(graph, texts.output, model=self.models["LinguisticObject"])

            self.add_serialization_chain(graph, items.output, model=self.models["VisualItem"])
            self.add_serialization_chain(graph, hmos1.output, model=self.models["HumanMadeObject"])
            self.add_serialization_chain(graph, hmos2.output, model=self.models["HumanMadeObject"])
            self.add_person_or_group_chain(graph, odata, key="_organizations")  # organizations are groups too!
            self.add_serialization_chain(graph, hmos2.output, model=self.models["HumanMadeObject"])
            self.add_serialization_chain(graph, texts.output, model=self.models["LinguisticObject"])
            self.add_person_or_group_chain(graph, artists)
            self.add_person_or_group_chain(graph, groups1)
            self.add_person_or_group_chain(graph, groups2)
            self.add_person_or_group_chain(graph, people)
            # self.add_person_or_group_chain(graph, people2)
            self.add_person_or_group_chain(graph, owners)
            # self.add_serialization_chain(graph, prov_entry.output, model=self.models["ProvenanceEntry"])
            _ = self.add_places_chain(graph, odata, key="_locations", serialize=serialize, include_self=True)
        return objects

    def add_books_chain(self, graph, sales_records, serialize=True):
        books = graph.add_chain(
            # add_book,
            AddBooks(static_instances=self.static_instances, helper=self.helper),
            _input=sales_records.output,
        )
        # phys = graph.add_chain(ExtractKeyedValue(key="_physical_book"), _input=books.output)

        textual_works = graph.add_chain(ExtractKeyedValues(key="_book_records"), _input=books.output)
        physical_objects = graph.add_chain(ExtractKeyedValues(key="_physical_books"), _input=books.output)
        if serialize:
            # self.add_serialization_chain(graph, act.output, model=self.models['ProvenanceEntry'])
            self.add_serialization_chain(graph, physical_objects.output, model=self.models["HumanMadeObject"])
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

        textual_works = graph.add_chain(ExtractKeyedValues(key="_records"), _input=rows.output)

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
