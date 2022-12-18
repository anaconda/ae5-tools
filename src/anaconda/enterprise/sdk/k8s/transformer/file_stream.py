import sys


class FileStream(object):
    def __init__(self, stream):
        self.stream = sys.stdout if stream is None else stream

    async def prepare(self, request):
        pass

    def closing(self):
        return False

    async def write(self, data):
        return self.stream.write(data.decode())

    async def finish(self):
        pass
