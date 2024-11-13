import re


class ErrorStringTester:
    def is_syntax_error(string):
        return string.startswith("SYNTAXERR") or \
            string.startswith("unknown subcommand")

    def is_nonexistent_error(string):
        return string.startswith("NONEXISTENT")

    def is_wrongtype_error(string):
        return string.startswith("WRONGTYPE")

    def is_number_overflow_error(string):
        return string.startswith("OVERFLOW")

    def is_outofboundaries_error(string):
        return string.startswith("OUTOFBOUNDARIES")

    def is_limit_exceeded_error(string):
        return string.startswith("LIMIT")

    def is_write_error(string):
        return string.startswith("ERROR") or string.startswith("OUTOFBOUNDARIES") or \
            string.startswith("WRONGTYPE") or string.startswith("NONEXISTENT")

    # NOTE: Uses .find instead of .startswith in case prefix added in the future
    def is_wrong_number_of_arguments_error(string):
        return string.find("wrong number of arguments") >= 0 or \
            string.lower().find('invalid number of arguments') >= 0
