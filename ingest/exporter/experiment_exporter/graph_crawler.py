from ingest.api.ingestapi import IngestApi
from ingest.exporter.metadata import MetadataResource

from copy import deepcopy
from typing import Any, List, Set, Iterable, Mapping, Dict
from functools import reduce
from _operator import iconcat
from dataclasses import dataclass


@dataclass
class ProtocolLink:
    protocol_type: str
    protocol_uuid: str

    @staticmethod
    def from_metadata_resource(metadata: MetadataResource) -> 'ProtocolLink':
        return ProtocolLink(metadata.concrete_type(), metadata.uuid)

    def to_dict(self) -> Dict:
        return {
            'protocol_type': self.protocol_type,
            'protocol_id': self.protocol_uuid
        }


@dataclass
class Link:
    process_uuid: str
    inputs: Set[str]
    input_type: str
    outputs: Set[str]
    output_type: str
    protocols: List[ProtocolLink]

    def add_output(self, output_uuid: str):
        self.outputs.add(output_uuid)

    def combine_partial_link(self, link: 'Link'):
        self.inputs = self.inputs.union(link.inputs)
        self.outputs = self.outputs.union(link.outputs)

    def to_dict(self) -> Dict:
        return {
            'process': self.process_uuid,
            'inputs': self.inputs,
            'input_type': self.input_type,
            'outputs': self.outputs,
            'output_type': self.output_type,
            'protocols': [protocol.to_dict() for protocol in self.protocols]
        }


@dataclass
class ProcessInputsAndProtocols:
    process: MetadataResource
    inputs: List[MetadataResource]
    protocols: List[MetadataResource]


class LinkSet:
    def __init__(self):
        self.links: Dict[str, Link] = dict()

    def add_links(self, links: List[Link]):
        for link in links:
            self.add_link(link)

    def add_link(self, link: Link):
        link_uuid = link.process_uuid
        if link_uuid in self.links:
            self.links[link_uuid].combine_partial_link(link)
        else:
            self.links[link_uuid] = link

    def get_links(self) -> List[Link]:
        return list(self.links.values())


class MetadataNodeSet:
    def __init__(self):
        self.obj_uuids = set()  # the uuid of a link is just the uuid of the process denoting the link
        self.objs = []

    def __contains__(self, item: MetadataResource):
        return item.uuid in self.obj_uuids

    def add_node(self, node: MetadataResource):
        if node.uuid in self.obj_uuids:
            pass
        else:
            self.obj_uuids.add(node.uuid)
            self.objs.append(node)

    def add_nodes(self, nodes: List[MetadataResource]):
        for node in nodes:
            self.add_node(node)

    def get_nodes(self) -> List[MetadataResource]:
        return [deepcopy(obj) for obj in self.objs]


class ExperimentGraph:
    links: LinkSet
    nodes: MetadataNodeSet

    def __init__(self):
        self.links = LinkSet()
        self.nodes = MetadataNodeSet()

    def extend(self, graph: 'ExperimentGraph'):
        for link in graph.links.get_links():
            self.links.add_link(link)

        for node in graph.nodes.get_nodes():
            self.nodes.add_node(node)

        return self


class GraphCrawler:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def experiment_graph_for_process(self, process: MetadataResource) -> ExperimentGraph:
        derived_biomaterials = self.get_derived_biomaterials(process)
        derived_files = self.get_derived_files(process)
        derived_entites = derived_biomaterials + derived_files

        return reduce(lambda pg1,pg2: pg1.extend(pg2),
                      map(lambda entity: self._crawl(entity, ExperimentGraph()), derived_entites),
                      ExperimentGraph())

    def _crawl(self, derived_node: MetadataResource, partial_graph: ExperimentGraph) -> ExperimentGraph:
        if derived_node in partial_graph.nodes:
            return partial_graph
        else:
            derived_by_processes = self.get_derived_by_processes(derived_node)
            processes_protocols_and_inputs = [self.inputs_and_protocols_for_process(process) for process in derived_by_processes]
            processes_links = GraphCrawler.links_for_processes(processes_protocols_and_inputs, derived_node)

            partial_graph.links.add_links(processes_links)
            partial_graph.nodes.add_nodes([derived_node] +
                                          GraphCrawler.flatten([p.protocols + [p.process] for p in processes_protocols_and_inputs]))

            all_process_inputs = GraphCrawler.flatten([p.inputs for p in processes_protocols_and_inputs])
            return reduce(lambda pg1,pg2: pg1.extend(pg2),
                          map(lambda input: self._crawl(input, partial_graph), all_process_inputs),
                          partial_graph)

    @staticmethod
    def link_for(process_uuid: str, input_uuids: List[str], input_type: str,
                 output_uuids: List[str], output_type: str, protocols: List[MetadataResource]) -> Link:
        protocol_links = [ProtocolLink.from_metadata_resource(protocol) for protocol in protocols]
        return Link(process_uuid, set(input_uuids), input_type, set(output_uuids), output_type, protocol_links)

    @staticmethod
    def flatten(list_of_lists: Iterable[Iterable]) -> List:
        return reduce(iconcat, list_of_lists, [])

    @staticmethod
    def links_for_processes(processes_protocols_and_inputs: List[ProcessInputsAndProtocols],
                            output: MetadataResource) -> List[Link]:
        links = []
        for process_protocol_and_inputs in processes_protocols_and_inputs:
            process_uuid = process_protocol_and_inputs.process.uuid
            links += GraphCrawler.links_for_process(process_uuid, output, process_protocol_and_inputs.protocols,
                                                    process_protocol_and_inputs.inputs)
        return links

    @staticmethod
    def links_for_process(process_uuid: str, output: MetadataResource,
                          protocols_for_process: List[MetadataResource],
                          inputs_for_process: List[MetadataResource]) -> List[Link]:
        output_uuid = output.uuid
        output_type = output.metadata_type
        file_input_uuids = [input.uuid for input in inputs_for_process if input.metadata_type == "file"]
        biomaterial_input_uuids = [input.uuid for input in inputs_for_process if input.metadata_type == "biomaterial"]

        links = []
        if len(file_input_uuids) > 0:
            links.append(GraphCrawler.link_for(process_uuid, file_input_uuids, "file",
                                               [output_uuid], output_type, protocols_for_process))

        if len(biomaterial_input_uuids) > 0:
            links.append(GraphCrawler.link_for(process_uuid, file_input_uuids, "biomaterial",
                                               [output_uuid], output_type, protocols_for_process))
        return links

    def get_derived_by_processes(self, experiment_material: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedByProcesses', experiment_material.full_resource, 'processes'))

    def get_derived_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedBiomaterials', process.full_resource, 'biomaterials'))

    def get_derived_files(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('derivedFiles', process.full_resource, 'files'))

    def get_input_biomaterials(self, process: MetadataResource):
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('inputBiomaterials', process.full_resource, 'biomaterials'))

    def get_input_files(self, process: MetadataResource):
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('inputFiles', process.full_resource, 'files'))

    def get_protocols(self, process: MetadataResource) -> List[MetadataResource]:
        return GraphCrawler.parse_metadata_resources(self.ingest_client.get_related_entities('protocols', process.full_resource, 'protocols'))

    def inputs_and_protocols_for_process(self, process: MetadataResource) -> ProcessInputsAndProtocols:
        return ProcessInputsAndProtocols(process,
                                         self.get_input_biomaterials(process) + self.get_input_files(process),
                                         self.get_protocols(process))

    @staticmethod
    def parse_metadata_resources(metadata_resources: List[Dict]) -> List[MetadataResource]:
        return [MetadataResource.from_dict(m, True) for m in metadata_resources]
