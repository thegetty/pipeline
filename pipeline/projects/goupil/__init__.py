import settings
import bonobo
import csv
import sys
from pipeline.projects import PersonIdentity, PipelineBase, UtilityHelper
from pipeline.util import MatchingFiles
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import KeyManagement, RecordCounter


class GoupilPersonIdentity(PersonIdentity):
    pass


class GoupilUtilityHelper(UtilityHelper):
    """
    Project-specific code for accessing and interpreting goupil data.
    """

    def __init__(self, project_name, static_instances=None):
        super().__init__(project_name)
        self.person_identity = GoupilPersonIdentity(
            make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri
        )


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
        return services

    def add_sales_chain(self, graph, records, services, serialize=True):
        """Add transformation of sales records to the bonobo pipeline."""

        sales_records = graph.add_chain(
            KeyManagement(
                drop_empty=True,
                operations=[
                    {
                        "group_repeating": {
                            "book_records": {
                                "rename_keys": {
                                    "stock_book_no": "no",
                                    "stock_book_gno": "gno",
                                    "stock_book_pg": "pg",
                                    "stock_book_row": "row",
                                },
                                "prefixes": ("stock_book_no", "stock_book_gno", "stock_book_pg", "stock_book_row"),
                            },
                            "artists": {
                                "rename_keys": {
                                    "artist_name": "name",
                                    "art_authority": "auth_name",
                                    "attribution_mod": "attrib_mod",
                                    "attribution_auth_mod": "attrib_mod_auth",
                                    "artist_ulan_id": "ulan_id",
                                },
                                "prefixes": (
                                    "artist_name",
                                    "art_authority",
                                    "attribution_mod",
                                    "attribution_auth_mod",
                                    "artist_ulan_id",
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
                                    "joint_own": "co_owner_name",
                                    "joint_own_sh": "co_owner_share",
                                    "joint_ulan_id": "co_owner_ulan_id",
                                },
                                "prefixes": ("joint_own", "joint_own_sh", "joint_ulan_id"),
                            },
                        }
                    }
                ],
            ),
            # RecordCounter(name='records', verbose=self.debug),
            _input=records.output,
        )

        return sales_records

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
    pass
