

edge_debug = True


def print_info(msg):
    if edge_debug:
        assert isinstance(msg, str)
        print msg

