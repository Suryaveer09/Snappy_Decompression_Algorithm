import hashlib

class Event:
    def _init_(self, payload):
        if not payload.endswith("\n"):
            # Append a newline to the output to make it easier to process by other tools
            self.payload = payload + "\n"
        else:
            self.payload = payload

    def _hash_(self):
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def _str_(self):
        return self.payload

    def to_byte_buffer(self):
        return bytes(self.payload, encoding="utf-8")