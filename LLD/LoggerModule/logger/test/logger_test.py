from logger.config_reader import LoggerConfigReader
from logger.logger import LoggerFactory


if __name__ == "__main__":
    # Read and validate configuration
    config = LoggerConfigReader.read_config('logger_config.yml')
    logger = LoggerFactory.create_logger(config)

    # Test logging with different priorities using convenience methods
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warn("This is a warning message")
    logger.error("This is an error message")
    logger.fatal("This is a fatal message")

    # Test concurrent logging
    def log_messages(logger, count):
        for i in range(count):
            logger.info(f"Concurrent message {i}")

    import threading
    threads = []
    for _ in range(5):
        t = threading.Thread(target=log_messages, args=(logger, 100))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Allow time for async processing
    import time
    time.sleep(2)

    logger.stop()
