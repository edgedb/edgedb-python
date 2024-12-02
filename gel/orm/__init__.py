import unittest

# No tests here, but we want to skip the unittest loader from attempting to
# import ORM packages which may not have been installed (like Django that has
# a few custom adjustments to make our models work).
def load_tests(loader, tests, pattern):
    return tests
