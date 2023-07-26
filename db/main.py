import sys

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


class MetaCommandResult(Enum):
    UNRECOGNIZED_COMMAND = 1


class StatementType(Enum):
    INSERT = 1
    SELECT = 2


class PrepareResult(Enum):
    SUCCESS = 1
    UNRECOGNIZED_STATEMENT = 2


def do_meta_command(input_buffer):
    if input_buffer.buffer == ".exit":
        sys.exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND


def prepare_statement(input_buffer, statement):
    if input_buffer.buffer.startswith("insert"):
        statement.type = StatementType.INSERT
        return PrepareResult.SUCCESS
    if input_buffer.buffer == "select":
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS

    return PrepareResult.UNRECOGNIZED_STATEMENT


class Statement:
    def __init__(self, statement_type):
        self.type = statement_type


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

        if input_buffer.buffer == ".exit":
            close_input_buffer(input_buffer)
            exit(0)
        else:
            print(f"Unrecognized command '{input_buffer.buffer}'.")


if __name__ == "__main__":
    main()
