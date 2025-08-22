# pipeline_lib/parsers/__init__.py
from . import structured_parser
from . import recursive_parser

# This is the Parser Registry
PARSER_REGISTRY = {
    'STRUCTURE_AWARE': structured_parser.parse_document,
    'RECURSIVE': recursive_parser.parse_document,
}

DEFAULT_PARSER = 'RECURSIVE'