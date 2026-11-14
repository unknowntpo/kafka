from ducktape.tests.test import Test


class PipefailCheckTest(Test):
    """Always fails. Used only to check that the opensource4you/clip kafka-e2e workflow reports a failed run."""

    def test_always_fails(self):
        assert False, "intentional failure"
