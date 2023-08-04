import sys
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
        print("This is where we would do an insert.")
    elif statement.type == "STATEMENT_SELECT":
        print("This is where we would do a select.")


def main():
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
            continue

        execute_statement(statement)
        print("Executed.")


if __name__ == "__main__":
    main()
