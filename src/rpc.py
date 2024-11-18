import logging
import functools
import inspect

from typing import Any, NamedTuple, Optional
from collections.abc import Callable

# TODO: These are generic functions that have some performance
# impact. Ideally, it would be nice to replce them with specific
# functions that will reduce overhead.
from mashumaro.codecs.json import json_decode, json_encode

import server


def rpc_call(func: Callable[[int, ...], Any]):
    # the endpoint of the function is the name of the function itself.
    endpoint = func.__name__.replace('_', '-')

    signature = inspect.signature(func)

    # convert from an ordered dict to a normal dict
    # order of arguments does not matter as we will be passing in
    # all arguments as named arguments.
    arg_signature = dict(signature.parameters)
    # construct the named tuple
    arg_type = {k: v.annotation for k, v in arg_signature.items()}
    argument_serde_type = NamedTuple("arguments", **arg_type)

    # since Nonetype is not supported by mashumaro, we need to
    # disguise our solution in the form of bullshit. If we find an
    # str, I think we are in greater trouble.
    return_annotation = signature._return_annotation or None | str

    @functools.wraps(func)
    def wrapper_func(__port: Optional[int], *args, **kwargs):
        if __port is None:
            return func(*args, **kwargs)

        # from the name, it is clear that the positional arguments map to the
        # respective arguments in order. Since the signature captures that exactly,
        # we can use that as is
        pos_params = {k: v for k, v in zip(signature.parameters.keys(), args)}
        # merge kwargs and pos_params to form the final dictionary
        func_params = pos_params | kwargs

        # serialise it
        # TODO: Use specialised decoders
        msg = json_encode(func_params, argument_serde_type)

        # Make the actual request
        print(server.node.peers)
        assert(__port in server.node.peers)
        msg_content = server.node.send_message(target=__port, endpoint=endpoint, msg=msg)

        return_vals = json_decode(msg_content, return_annotation) if msg_content is not None else None
        return return_vals

    def handler(msg: bytes) -> bytes:
        # deserialise incoming message
        args = json_decode(msg, argument_serde_type)
        # call function
        out = func(**args)
        # return output
        return json_encode(out, return_annotation)

    server.node.register_endpoint(endpoint, handler)

    return wrapper_func
