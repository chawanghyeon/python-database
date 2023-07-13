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


def print_prompt():
    print("db > ", end="")


def read_input(input_buffer):
    input_buffer.buffer = input()
    input_buffer.input_length = len(input_buffer.buffer)
    input_buffer.buffer_length = (
        input_buffer.input_length + 1
    )  # Plus one for the newline


def main():
    input_buffer = InputBuffer()
    while True:
        print_prompt()
        read_input(input_buffer)

        if input_buffer.buffer == ".exit":
            exit(0)
        else:
            print(f"Unrecognized command '{input_buffer.buffer}'.")


if __name__ == "__main__":
    main()
