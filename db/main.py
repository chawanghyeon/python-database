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

import sys
import struct

LEAF_NODE_KEY_SIZE = 4  # Assuming 4 bytes as uint32_t
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE  # Assuming ROW_SIZE is predefined
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = (
    PAGE_SIZE - LEAF_NODE_HEADER_SIZE
)  # Assuming PAGE_SIZE and LEAF_NODE_HEADER_SIZE are predefined
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS // LEAF_NODE_CELL_SIZE
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT


class Node:
    def __init__(self):
        self.data = bytearray()  # Placeholder for void* node equivalent

    def get_node_type(
        self, NODE_TYPE_OFFSET
    ):  # Assuming NODE_TYPE_OFFSET is predefined
        return struct.unpack("B", self.data[NODE_TYPE_OFFSET : NODE_TYPE_OFFSET + 1])[0]

    def set_node_type(
        self, type, NODE_TYPE_OFFSET
    ):  # Assuming NODE_TYPE_OFFSET is predefined
        self.data[NODE_TYPE_OFFSET : NODE_TYPE_OFFSET + 1] = struct.pack("B", type)

    def is_node_root(self, IS_ROOT_OFFSET):  # Assuming IS_ROOT_OFFSET is predefined
        return bool(
            struct.unpack("B", self.data[IS_ROOT_OFFSET : IS_ROOT_OFFSET + 1])[0]
        )

    def set_node_root(
        self, is_root, IS_ROOT_OFFSET
    ):  # Assuming IS_ROOT_OFFSET is predefined
        self.data[IS_ROOT_OFFSET : IS_ROOT_OFFSET + 1] = struct.pack("B", int(is_root))

    def node_parent(
        self, PARENT_POINTER_OFFSET
    ):  # Assuming PARENT_POINTER_OFFSET is predefined
        return struct.unpack(
            "I", self.data[PARENT_POINTER_OFFSET : PARENT_POINTER_OFFSET + 4]
        )[0]

    def internal_node_num_keys(
        self, INTERNAL_NODE_NUM_KEYS_OFFSET
    ):  # Assuming INTERNAL_NODE_NUM_KEYS_OFFSET is predefined
        return struct.unpack(
            "I",
            self.data[
                INTERNAL_NODE_NUM_KEYS_OFFSET : INTERNAL_NODE_NUM_KEYS_OFFSET + 4
            ],
        )[0]

    def internal_node_right_child(
        self, INTERNAL_NODE_RIGHT_CHILD_OFFSET
    ):  # Assuming INTERNAL_NODE_RIGHT_CHILD_OFFSET is predefined
        return struct.unpack(
            "I",
            self.data[
                INTERNAL_NODE_RIGHT_CHILD_OFFSET : INTERNAL_NODE_RIGHT_CHILD_OFFSET + 4
            ],
        )[0]

    def internal_node_cell(
        self, cell_num, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_CELL_SIZE
    ):  # Assuming INTERNAL_NODE_HEADER_SIZE and INTERNAL_NODE_CELL_SIZE are predefined
        return self.data[
            INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE :
        ]

    def internal_node_child(
        self,
        child_num,
        INTERNAL_NODE_NUM_KEYS_OFFSET,
        INTERNAL_NODE_RIGHT_CHILD_OFFSET,
        INTERNAL_NODE_HEADER_SIZE,
        INTERNAL_NODE_CELL_SIZE,
        INVALID_PAGE_NUM,
    ):  # Assuming INTERNAL_NODE_NUM_KEYS_OFFSET, INTERNAL_NODE_RIGHT_CHILD_OFFSET, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_CELL_SIZE and INVALID_PAGE_NUM are predefined
        num_keys = self.internal_node_num_keys(INTERNAL_NODE_NUM_KEYS_OFFSET)
        if child_num > num_keys:
            sys.exit(f"Tried to access child_num {child_num} > num_keys {num_keys}")
        elif child_num == num_keys:
            right_child = self.internal_node_right_child(
                INTERNAL_NODE_RIGHT_CHILD_OFFSET
            )
            if right_child == INVALID_PAGE_NUM:
                sys.exit("Tried to access right child of node, but was invalid page")
            return right_child
        else:
            child = self.internal_node_cell(
                child_num, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_CELL_SIZE
            )
            if child == INVALID_PAGE_NUM:
                sys.exit(
                    f"Tried to access child {child_num} of node, but was invalid page"
                )
            return child

    def internal_node_key(
        self, key_num, INTERNAL_NODE_CHILD_SIZE
    ):  # Assuming INTERNAL_NODE_CHILD_SIZE is predefined
        return self.internal_node_cell(
            key_num, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_CELL_SIZE
        )[INTERNAL_NODE_CHILD_SIZE:]

    def leaf_node_num_cells(
        self, LEAF_NODE_NUM_CELLS_OFFSET
    ):  # Assuming LEAF_NODE_NUM_CELLS_OFFSET is predefined
        return struct.unpack(
            "I", self.data[LEAF_NODE_NUM_CELLS_OFFSET : LEAF_NODE_NUM_CELLS_OFFSET + 4]
        )[0]

    def leaf_node_next_leaf(
        self, LEAF_NODE_NEXT_LEAF_OFFSET
    ):  # Assuming LEAF_NODE_NEXT_LEAF_OFFSET is predefined
        return struct.unpack(
            "I", self.data[LEAF_NODE_NEXT_LEAF_OFFSET : LEAF_NODE_NEXT_LEAF_OFFSET + 4]
        )[0]

    def leaf_node_cell(
        self, cell_num, LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE
    ):  # Assuming LEAF_NODE_HEADER_SIZE and LEAF_NODE_CELL_SIZE are predefined
        return self.data[LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE :]

    def leaf_node_key(
        self, cell_num, LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE
    ):  # Assuming LEAF_NODE_HEADER_SIZE and LEAF_NODE_CELL_SIZE are predefined
        return self.leaf_node_cell(cell_num, LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE)

    def leaf_node_value(
        self, cell_num, LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE, LEAF_NODE_KEY_SIZE
    ):  # Assuming LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE and LEAF_NODE_KEY_SIZE are predefined
        return self.leaf_node_cell(
            cell_num, LEAF_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE
        )[LEAF_NODE_KEY_SIZE:]
