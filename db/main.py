from enum import Enum


class InputBuffer:
    def __init__(self):
        self.buffer = None
        self.buffer_length = 0
        self.input_length = 0


class ExecuteResult(Enum):
    EXECUTE_SUCCESS = 0
    EXECUTE_DUPLICATE_KEY = 1


class MetaCommandResult(Enum):
    META_COMMAND_SUCCESS = 0
    META_COMMAND_UNRECOGNIZED_COMMAND = 1


class PrepareResult(Enum):
    PREPARE_SUCCESS = 0
    PREPARE_NEGATIVE_ID = 1
    PREPARE_STRING_TOO_LONG = 2
    PREPARE_SYNTAX_ERROR = 3
    PREPARE_UNRECOGNIZED_STATEMENT = 4


class StatementType(Enum):
    STATEMENT_INSERT = 0
    STATEMENT_SELECT = 1


from enum import Enum

COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255


class StatementType(Enum):
    STATEMENT_INSERT = 0
    STATEMENT_SELECT = 1


class Row:
    def __init__(self):
        self.id = 0
        self.username = " " * (COLUMN_USERNAME_SIZE + 1)
        self.email = " " * (COLUMN_EMAIL_SIZE + 1)


class Statement:
    def __init__(self):
        self.type = StatementType.STATEMENT_INSERT
        self.row_to_insert = Row()  # only used by insert statement


ID_SIZE = (
    USERNAME_SIZE
) = EMAIL_SIZE = 4  # assuming 4 bytes for each integer and char array references
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 400
INVALID_PAGE_NUM = 4294967295  # equivalent to UINT32_MAX


class Pager:
    def __init__(self):
        self.file_descriptor = None
        self.file_length = 0
        self.num_pages = 0
        self.pages = [None] * TABLE_MAX_PAGES


class Table:
    def __init__(self):
        self.pager = Pager()
        self.root_page_num = 0


class Cursor:
    def __init__(self):
        self.table = Table()
        self.page_num = 0
        self.cell_num = 0
        self.end_of_table = False  # Indicates a position one past the last element


def print_row(row):
    print("({}, {}, {})".format(row.id, row.username.strip(), row.email.strip()))


class NodeType(Enum):
    NODE_INTERNAL = 0
    NODE_LEAF = 1


NODE_TYPE_SIZE = IS_ROOT_SIZE = PARENT_POINTER_SIZE = 4  # assuming 4 bytes for each
NODE_TYPE_OFFSET = 0
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

INTERNAL_NODE_NUM_KEYS_SIZE = (
    INTERNAL_NODE_RIGHT_CHILD_SIZE
) = 4  # assuming 4 bytes for each
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_OFFSET = (
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
)
INTERNAL_NODE_HEADER_SIZE = (
    COMMON_NODE_HEADER_SIZE
    + INTERNAL_NODE_NUM_KEYS_SIZE
    + INTERNAL_NODE_RIGHT_CHILD_SIZE
)

INTERNAL_NODE_KEY_SIZE = INTERNAL_NODE_CHILD_SIZE = 4  # assuming 4 bytes for each
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
INTERNAL_NODE_MAX_KEYS = 3

LEAF_NODE_NUM_CELLS_SIZE = LEAF_NODE_NEXT_LEAF_SIZE = 4  # assuming 4 bytes for each
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
LEAF_NODE_HEADER_SIZE = (
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
)
