import sys


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
