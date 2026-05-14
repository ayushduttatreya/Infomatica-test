"""
Extract Informatica repository mappings from XML file.
Parses Informatica PowerCenter repository XML to extract mapping definitions,
source/target definitions, and transformation logic.
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
import json

logger = logging.getLogger(__name__)


@dataclass
class SourceField:
    """Represents a source field definition."""
    name: str
    datatype: str
    length: int
    nullable: str
    precision: int
    scale: int
    field_number: int
    key_type: str = "NOT A KEY"
    description: str = ""
    business_name: str = ""


@dataclass
class SourceDefinition:
    """Represents a source definition."""
    name: str
    database_type: str
    description: str
    fields: List[SourceField] = field(default_factory=list)
    object_version: str = "1"
    version_number: str = "1"


@dataclass
class TargetField:
    """Represents a target field definition."""
    name: str
    datatype: str
    length: int
    nullable: str
    precision: int
    scale: int
    field_number: int
    key_type: str = "NOT A KEY"
    description: str = ""


@dataclass
class TargetDefinition:
    """Represents a target definition."""
    name: str
    database_type: str
    description: str
    fields: List[TargetField] = field(default_factory=list)
    object_version: str = "1"


@dataclass
class TransformField:
    """Represents a transformation field."""
    name: str
    datatype: str
    precision: int
    scale: int
    expression: str = ""
    description: str = ""
    port_type: str = ""


@dataclass
class Transformation:
    """Represents a transformation definition."""
    name: str
    type: str
    description: str
    fields: List[TransformField] = field(default_factory=list)
    object_version: str = "1"
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingInstance:
    """Represents a mapping instance (source/target/transformation usage)."""
    name: str
    type: str
    transformation_name: str
    transformation_type: str


@dataclass
class MappingConnector:
    """Represents a connector between transformations."""
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class Mapping:
    """Represents a complete mapping definition."""
    name: str
    description: str
    is_valid: str
    instances: List[MappingInstance] = field(default_factory=list)
    connectors: List[MappingConnector] = field(default_factory=list)
    object_version: str = "1"


@dataclass
class Folder:
    """Represents a repository folder."""
    name: str
    owner: str
    description: str
    sources: List[SourceDefinition] = field(default_factory=list)
    targets: List[TargetDefinition] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    mappings: List[Mapping] = field(default_factory=list)
    version: str = "1"


@dataclass
class Repository:
    """Represents the complete Informatica repository."""
    name: str
    version: str
    codepage: str
    database_type: str
    folders: List[Folder] = field(default_factory=list)
    creation_date: str = ""
    repository_version: str = ""


class InformaticaXMLParser:
    """Parser for Informatica PowerCenter repository XML files."""

    def __init__(self, xml_path: str):
        """
        Initialize the parser.
        
        Args:
            xml_path: Path to the Informatica repository XML file
        """
        self.xml_path = Path(xml_path)
        self.tree: Optional[ET.ElementTree] = None
        self.root: Optional[ET.Element] = None

    def parse(self) -> Repository:
        """
        Parse the XML file and extract repository structure.
        
        Returns:
            Repository object containing all parsed definitions
            
        Raises:
            FileNotFoundError: If XML file does not exist
            ET.ParseError: If XML is malformed
        """
        logger.info(f"Parsing Informatica repository XML: {self.xml_path}")
        
        if not self.xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {self.xml_path}")
        
        try:
            self.tree = ET.parse(self.xml_path)
            self.root = self.tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            raise
        
        if self.root.tag != "POWERMART":
            raise ValueError(f"Invalid root element: {self.root.tag}, expected POWERMART")
        
        repository = self._parse_repository()
        logger.info(f"Successfully parsed repository: {repository.name}")
        logger.info(f"Found {len(repository.folders)} folders")
        
        return repository

    def _parse_repository(self) -> Repository:
        """Parse the REPOSITORY element."""
        repo_elem = self.root.find("REPOSITORY")
        if repo_elem is None:
            raise ValueError("No REPOSITORY element found in XML")
        
        repository = Repository(
            name=repo_elem.get("NAME", ""),
            version=repo_elem.get("VERSION", ""),
            codepage=repo_elem.get("CODEPAGE", "UTF-8"),
            database_type=repo_elem.get("DATABASETYPE", ""),
            creation_date=self.root.get("CREATION_DATE", ""),
            repository_version=self.root.get("REPOSITORY_VERSION", "")
        )
        
        for folder_elem in repo_elem.findall("FOLDER"):
            folder = self._parse_folder(folder_elem)
            repository.folders.append(folder)
        
        return repository

    def _parse_folder(self, folder_elem: ET.Element) -> Folder:
        """Parse a FOLDER element."""
        folder = Folder(
            name=folder_elem.get("NAME", ""),
            owner=folder_elem.get("OWNER", ""),
            description=folder_elem.get("DESCRIPTION", ""),
            version=folder_elem.get("VERSION", "1")
        )
        
        logger.info(f"Parsing folder: {folder.name}")
        
        # Parse sources
        for source_elem in folder_elem.findall("SOURCE"):
            source = self._parse_source(source_elem)
            folder.sources.append(source)
        
        # Parse targets
        for target_elem in folder_elem.findall("TARGET"):
            target = self._parse_target(target_elem)
            folder.targets.append(target)
        
        # Parse transformations
        for transform_elem in folder_elem.findall("TRANSFORMATION"):
            transformation = self._parse_transformation(transform_elem)
            folder.transformations.append(transformation)
        
        # Parse mappings
        for mapping_elem in folder_elem.findall("MAPPING"):
            mapping = self._parse_mapping(mapping_elem)
            folder.mappings.append(mapping)
        
        logger.info(f"Folder {folder.name}: {len(folder.sources)} sources, "
                   f"{len(folder.targets)} targets, "
                   f"{len(folder.transformations)} transformations, "
                   f"{len(folder.mappings)} mappings")
        
        return folder

    def _parse_source(self, source_elem: ET.Element) -> SourceDefinition:
        """Parse a SOURCE element."""
        source = SourceDefinition(
            name=source_elem.get("NAME", ""),
            database_type=source_elem.get("DATABASETYPE", ""),
            description=source_elem.get("DESCRIPTION", ""),
            object_version=source_elem.get("OBJECTVERSION", "1"),
            version_number=source_elem.get("VERSIONNUMBER", "1")
        )
        
        for field_elem in source_elem.findall("SOURCEFIELD"):
            field = SourceField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                length=int(field_elem.get("LENGTH", "0")),
                nullable=field_elem.get("NULLABLE", "NULL"),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                field_number=int(field_elem.get("FIELDNUMBER", "0")),
                key_type=field_elem.get("KEYTYPE", "NOT A KEY"),
                description=field_elem.get("DESCRIPTION", ""),
                business_name=field_elem.get("BUSINESSNAME", "")
            )
            source.fields.append(field)
        
        return source

    def _parse_target(self, target_elem: ET.Element) -> TargetDefinition:
        """Parse a TARGET element."""
        target = TargetDefinition(
            name=target_elem.get("NAME", ""),
            database_type=target_elem.get("DATABASETYPE", ""),
            description=target_elem.get("DESCRIPTION", ""),
            object_version=target_elem.get("OBJECTVERSION", "1")
        )
        
        for field_elem in target_elem.findall("TARGETFIELD"):
            field = TargetField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                length=int(field_elem.get("LENGTH", "0")),
                nullable=field_elem.get("NULLABLE", "NULL"),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                field_number=int(field_elem.get("FIELDNUMBER", "0")),
                key_type=field_elem.get("KEYTYPE", "NOT A KEY"),
                description=field_elem.get("DESCRIPTION", "")
            )
            target.fields.append(field)
        
        return target

    def _parse_transformation(self, transform_elem: ET.Element) -> Transformation:
        """Parse a TRANSFORMATION element."""
        transformation = Transformation(
            name=transform_elem.get("NAME", ""),
            type=transform_elem.get("TYPE", ""),
            description=transform_elem.get("DESCRIPTION", ""),
            object_version=transform_elem.get("OBJECTVERSION", "1")
        )
        
        # Parse transformation fields
        for field_elem in transform_elem.findall("TRANSFORMFIELD"):
            field = TransformField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                expression=field_elem.get("EXPRESSION", ""),
                description=field_elem.get("DESCRIPTION", ""),
                port_type=field_elem.get("PORTTYPE", "")
            )
            transformation.fields.append(field)
        
        # Parse table attributes (properties)
        for attr_elem in transform_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "")
            attr_value = attr_elem.get("VALUE", "")
            if attr_name:
                transformation.properties[attr_name] = attr_value
        
        return transformation

    def _parse_mapping(self, mapping_elem: ET.Element) -> Mapping:
        """Parse a MAPPING element."""
        mapping = Mapping(
            name=mapping_elem.get("NAME", ""),
            description=mapping_elem.get("DESCRIPTION", ""),
            is_valid=mapping_elem.get("ISVALID", "YES"),
            object_version=mapping_elem.get("OBJECTVERSION", "1")
        )
        
        # Parse instances
        for instance_elem in mapping_elem.findall("INSTANCE"):
            instance = MappingInstance(
                name=instance_elem.get("NAME", ""),
                type=instance_elem.get("TYPE", ""),
                transformation_name=instance_elem.get("TRANSFORMATION_NAME", ""),
                transformation_type=instance_elem.get("TRANSFORMATION_TYPE", "")
            )
            mapping.instances.append(instance)
        
        # Parse connectors
        for connector_elem in mapping_elem.findall("CONNECTOR"):
            connector = MappingConnector(
                from_instance=connector_elem.get("FROMINSTANCE", ""),
                from_field=connector_elem.get("FROMFIELD", ""),
                to_instance=connector_elem.get("TOINSTANCE", ""),
                to_field=connector_elem.get("TOFIELD", "")
            )
            mapping.connectors.append(connector)
        
        return mapping

    def export_to_json(self, repository: Repository, output_path: str) -> None:
        """
        Export parsed repository to JSON format.
        
        Args:
            repository: Parsed repository object
            output_path: Path to output JSON file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert dataclasses to dict
        repo_dict = self._to_dict(repository)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(repo_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported repository to JSON: {output_file}")

    def _to_dict(self, obj: Any) -> Any:
        """Recursively convert dataclass objects to dictionaries."""
        if hasattr(obj, '__dataclass_fields__'):
            result = {}
            for field_name, field_value in asdict(obj).items():
                result[field_name] = self._to_dict(field_value)
            return result
        elif isinstance(obj, list):
            return [self._to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._to_dict(value) for key, value in obj.items()}
        else:
            return obj


def extract_informatica_repository(xml_path: str, output_json_path: Optional[str] = None) -> Repository:
    """
    Main extraction function to parse Informatica repository XML.
    
    Args:
        xml_path: Path to Informatica repository XML file
        output_json_path: Optional path to export JSON output
        
    Returns:
        Parsed Repository object
    """
    parser = InformaticaXMLParser(xml_path)
    repository = parser.parse()
    
    if output_json_path:
        parser.export_to_json(repository, output_json_path)
    
    return repository


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example usage
    repo = extract_informatica_repository(
        "mappings/informatica_repository.xml",
        "output/repository_parsed.json"
    )
    print(f"Parsed repository: {repo.name}")
    print(f"Total folders: {len(repo.folders)}")