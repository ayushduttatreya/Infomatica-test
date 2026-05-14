"""
Extract customer master data from Informatica repository XML.
Parses source definitions and mapping transformations.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import logging

logging.basicConfig(level=logging.INFO)
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
    description: str = ""


@dataclass
class SourceDefinition:
    """Represents a source definition."""
    name: str
    database_type: str
    description: str
    fields: List[SourceField] = field(default_factory=list)


@dataclass
class InformaticaMapping:
    """Represents extracted Informatica mapping metadata."""
    repository_name: str
    folder_name: str
    sources: Dict[str, SourceDefinition] = field(default_factory=dict)


class InformaticaXMLExtractor:
    """Extracts metadata from Informatica repository XML."""
    
    def __init__(self, xml_path: str):
        self.xml_path = xml_path
        self.tree = None
        self.root = None
    
    def parse(self) -> InformaticaMapping:
        """Parse the Informatica XML file."""
        try:
            self.tree = ET.parse(self.xml_path)
            self.root = self.tree.getroot()
            logger.info(f"Successfully parsed XML file: {self.xml_path}")
            
            mapping = InformaticaMapping(
                repository_name=self._get_repository_name(),
                folder_name=self._get_folder_name()
            )
            
            mapping.sources = self._extract_sources()
            
            logger.info(f"Extracted {len(mapping.sources)} source definitions")
            return mapping
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error parsing Informatica XML: {e}")
            raise
    
    def _get_repository_name(self) -> str:
        """Extract repository name."""
        repo = self.root.find('.//REPOSITORY')
        return repo.get('NAME', 'UNKNOWN') if repo is not None else 'UNKNOWN'
    
    def _get_folder_name(self) -> str:
        """Extract folder name."""
        folder = self.root.find('.//FOLDER')
        return folder.get('NAME', 'UNKNOWN') if folder is not None else 'UNKNOWN'
    
    def _extract_sources(self) -> Dict[str, SourceDefinition]:
        """Extract all source definitions."""
        sources = {}
        
        for source_elem in self.root.findall('.//SOURCE'):
            source_def = self._parse_source_definition(source_elem)
            sources[source_def.name] = source_def
            logger.info(f"Extracted source: {source_def.name} with {len(source_def.fields)} fields")
        
        return sources
    
    def _parse_source_definition(self, source_elem: ET.Element) -> SourceDefinition:
        """Parse a single source definition."""
        source_def = SourceDefinition(
            name=source_elem.get('NAME', ''),
            database_type=source_elem.get('DATABASETYPE', ''),
            description=source_elem.get('DESCRIPTION', '')
        )
        
        for field_elem in source_elem.findall('.//SOURCEFIELD'):
            field = self._parse_source_field(field_elem)
            source_def.fields.append(field)
        
        return source_def
    
    def _parse_source_field(self, field_elem: ET.Element) -> SourceField:
        """Parse a single source field."""
        return SourceField(
            name=field_elem.get('NAME', ''),
            datatype=field_elem.get('DATATYPE', 'string'),
            length=int(field_elem.get('LENGTH', '0')),
            nullable=field_elem.get('NULLABLE', 'NULL'),
            precision=int(field_elem.get('PRECISION', '0')),
            scale=int(field_elem.get('SCALE', '0')),
            field_number=int(field_elem.get('FIELDNUMBER', '0')),
            description=field_elem.get('DESCRIPTION', '')
        )
    
    def get_source_schema(self, source_name: str) -> Optional[Dict]:
        """Get schema information for a specific source."""
        mapping = self.parse()
        source = mapping.sources.get(source_name)
        
        if not source:
            logger.warning(f"Source {source_name} not found")
            return None
        
        schema = {
            'name': source.name,
            'type': source.database_type,
            'description': source.description,
            'fields': [
                {
                    'name': field.name,
                    'type': field.datatype,
                    'length': field.length,
                    'nullable': field.nullable == 'NULL',
                    'precision': field.precision,
                    'scale': field.scale
                }
                for field in sorted(source.fields, key=lambda f: f.field_number)
            ]
        }
        
        return schema


def extract_customer_schema(xml_path: str) -> Dict:
    """Extract customer master data schema from Informatica XML."""
    extractor = InformaticaXMLExtractor(xml_path)
    return extractor.get_source_schema('SRC_CUSTOMERS')


def extract_all_schemas(xml_path: str) -> Dict[str, Dict]:
    """Extract all source schemas from Informatica XML."""
    extractor = InformaticaXMLExtractor(xml_path)
    mapping = extractor.parse()
    
    schemas = {}
    for source_name in mapping.sources.keys():
        schemas[source_name] = extractor.get_source_schema(source_name)
    
    return schemas