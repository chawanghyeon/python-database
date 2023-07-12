class InputBuffer:
    def __init__(self):
        self.buffer = None
        self.buffer_length = 0
        self.input_length = 0


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
