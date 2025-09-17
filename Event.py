import hashlib

class Event:
    def __init__(self, payload):
        if not payload.endswith("\n"):
            # Append a newline to the output to make it easier to process by other tools
            self.payload = payload + "\n"
        else:
            self.payload = payload

    def __hash__(self):
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def __str__(self):
        return self.payload

    def to_byte_buffer(self):
        return bytes(self.payload, encoding="utf-8")