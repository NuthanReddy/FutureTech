from logger.Utils.Sink import Sink


class ConsoleSink(Sink):
    def write(self, message: str):
        print(message)
