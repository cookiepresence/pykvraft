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

    @functools.wraps(func)
    def wrapper_func(__port: Optional[int], *args, **kwargs):
        if __port is None:
            return func(*args, **kwargs)

        # from the name, it is clear that the positional arguments map to the
        # respective arguments in order. Since the signature captures that exactly,
        # we can use that as is
        pos_params = {k: v for k, v in zip(signature.params.keys(), args)}
        # merge kwargs and pos_params to form the final dictionary
        func_params = pos_params | kwargs

        # serialise it
        # TODO: Use specialised decoders
        msg = json_encode(func_params, argument_serde_type)

        # Make the actual request
        msg_content = server.Node().send_message(__port, endpoint, msg)

        return_vals = json_decode(msg_content, signature.return_annotation)
        return return_vals

    def handler(msg: bytes) -> bytes:
        # deserialise incoming message
        args = json_decode(msg, argument_serde_type)
        # call function
        out = func(**args)
        # return output
        return json_encode(out, signature.return_annotation)

    # register with node!
    # check if node exists
    if (node := server.Node._instance) is not None:
        # if node exists, register
        node.register_endpoint(endpoint, handler)
    else:
        # otherwise, mark for registration
        server.Node._on_instance_creation.append(lambda node: node.register_endpoint(endpoint, handler))

    return wrapper_func
