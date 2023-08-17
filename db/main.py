import sys
import struct
from enum import Enum


class InputBuffer:
    def __init__(self):
        self.buffer = ""
        self.buffer_length = 0
        self.input_length = 0


def print_prompt():
    print("db > ", end="")


def read_input(input_buffer):
    line = sys.stdin.readline()
    bytes_read = len(line)

    if bytes_read <= 0:
        print("Error reading input")
        exit(1)

    # Ignore trailing newline
    input_buffer.input_length = bytes_read - 1
    input_buffer.buffer = line[:-1]
    input_buffer.buffer_length = len(input_buffer.buffer)


def close_input_buffer(input_buffer):
    input_buffer.buffer = None


class Row:
    def __init__(self):
        self.id = 0
        self.username = ""
        self.email = ""


ID_FORMAT = "I"  # uint32 in Python's struct module
USERNAME_FORMAT = "32s"  # 32-byte string
EMAIL_FORMAT = "255s"  # 255-byte string

ID_SIZE = struct.calcsize(ID_FORMAT)
USERNAME_SIZE = struct.calcsize(USERNAME_FORMAT)
EMAIL_SIZE = struct.calcsize(EMAIL_FORMAT)

ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE


def row_slot(table, row_num):
    page_num = row_num // ROWS_PER_PAGE
    try:
        page = table.pages[page_num]
    except IndexError:
        page = bytearray(PAGE_SIZE)
        table.pages[page_num] = page

    if page is None:
        page = bytearray(PAGE_SIZE)
        table.pages[page_num] = page

    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return page[byte_offset : byte_offset + ROW_SIZE]


class ExecuteResult(Enum):
    TABLE_FULL = 1
    SUCCESS = 2


def execute_insert(statement, table):
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.TABLE_FULL

    row_to_insert = statement.row_to_insert
    serialized_row = serialize_row(row_to_insert)
    slot = row_slot(table, table.num_rows)
    slot[
        : len(serialized_row)
    ] = serialized_row  # Copy the serialized row into the slot
    table.num_rows += 1

    return ExecuteResult.SUCCESS


def execute_select(statement, table):
    for i in range(table.num_rows):
        slot = row_slot(table, i)
        row = deserialize_row(slot)
        print_row(row)
    return ExecuteResult.SUCCESS


def print_row(row):
    # This function prints the row. You might need to define the exact printing logic.
    print(f"ID: {row.id}, Username: {row.username}, Email: {row.email}")


def serialize_row(source: Row, destination: bytearray):
    packed_id = struct.pack(ID_FORMAT, source.id)
    packed_username = struct.pack(USERNAME_FORMAT, source.username.encode())
    packed_email = struct.pack(EMAIL_FORMAT, source.email.encode())

    destination[ID_OFFSET : ID_OFFSET + ID_SIZE] = packed_id
    destination[USERNAME_OFFSET : USERNAME_OFFSET + USERNAME_SIZE] = packed_username
    destination[EMAIL_OFFSET : EMAIL_OFFSET + EMAIL_SIZE] = packed_email


def deserialize_row(source: bytearray) -> Row:
    row = Row()
    row.id = struct.unpack(ID_FORMAT, source[ID_OFFSET : ID_OFFSET + ID_SIZE])[0]
    row.username = (
        struct.unpack(
            USERNAME_FORMAT, source[USERNAME_OFFSET : USERNAME_OFFSET + USERNAME_SIZE]
        )[0]
        .decode()
        .strip("\x00")
    )
    row.email = (
        struct.unpack(EMAIL_FORMAT, source[EMAIL_OFFSET : EMAIL_OFFSET + EMAIL_SIZE])[0]
        .decode()
        .strip("\x00")
    )
    return row


PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


class Table:
    def __init__(self):
        self.num_rows = 0
        self.pages = [None] * TABLE_MAX_PAGES


class MetaCommandResult(Enum):
    UNRECOGNIZED_COMMAND = 1
    SUCCESS = 2


class StatementType(Enum):
    INSERT = 1
    SELECT = 2


class PrepareResult(Enum):
    UNRECOGNIZED_STATEMENT = 1
    SUCCESS = 2


def do_meta_command(input_buffer: InputBuffer) -> MetaCommandResult:
    if input_buffer.buffer == ".exit":
        sys.exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND


def prepare_statement(input_buffer, statement):
    if input_buffer.buffer.startswith("insert"):
        try:
            parts = input_buffer.buffer.split(" ")
            statement.type = StatementType.INSERT
            statement.row_to_insert.id = int(parts[1])
            statement.row_to_insert.username = parts[2]
            statement.row_to_insert.email = parts[3]
        except:
            return PrepareResult.SYNTAX_ERROR
        return PrepareResult.SUCCESS
    if input_buffer.buffer == "select":
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS

    return PrepareResult.UNRECOGNIZED_STATEMENT


class Statement:
    def __init__(self, statement_type):
        self.type = statement_type
        self.row_to_insert = Row()


def execute_statement(statement):
    if statement.type == "STATEMENT_INSERT":
        return execute_insert(statement)
    elif statement.type == "STATEMENT_SELECT":
        return execute_select(statement)


def new_table():
    return Table()


def free_table(table):
    table.num_rows = 0
    table.pages = [None] * TABLE_MAX_PAGES


def main():
    table = new_table()
    input_buffer = InputBuffer()
    while True:
        print_prompt()
        read_input(input_buffer)

        if input_buffer.buffer[0] == ".":
            meta_command_result = do_meta_command(input_buffer)
            if meta_command_result == MetaCommandResult.UNRECOGNIZED_COMMAND:
                print("Unrecognized command '{}'.".format(input_buffer.buffer))
                continue
            elif meta_command_result == MetaCommandResult.SUCCESS:
                continue

        statement = Statement()
        prepare_result = prepare_statement(input_buffer, statement)
        if prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT:
            print("Unrecognized keyword at start of '{}'.".format(input_buffer.buffer))
            continue
        elif prepare_result == PrepareResult.SUCCESS:
            break
        elif prepare_result == PrepareResult.SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
            continue

        execute_result = execute_statement(statement)
        if execute_result == ExecuteResult.TABLE_FULL:
            print("Error: Table full.")
            break
        elif execute_result == ExecuteResult.SUCCESS:
            print("Executed.")
            break


if __name__ == "__main__":
    main()
