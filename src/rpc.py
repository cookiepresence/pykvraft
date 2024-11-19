import logging
import functools
import inspect
import sys

from typing import Any, NamedTuple, Optional
from collections.abc import Callable

# TODO: These are generic functions that have some performance
# impact. Ideally, it would be nice to replce them with specific
# functions that will reduce overhead.
from mashumaro.codecs.msgpack import msgpack_decode, msgpack_encode

import node


def rpc_call(func: Callable[[int, ...], Any], ):
    """
    This is a decorator that can automatically register rpc calls and handle
    them without having to deal with networking/serialization/deserialization.

    This ensures that we are able to call functions in a type-safe way, eliminating
    an entire class of bugs and reducing the uncertainity of things.

    Example:

    @rpc_call
    def Hello(name: str) -> str:
      return f"Hello {name}!"

    This can be called like normal, along with providing the port to use.

    print(Hello(8080, "rb"))   # calls the rpc on port 8080
    print(Hello(None, "mon"))  # since the port is None, it is called locally and returned

    Note that the first parameter changes, but the rest of the function call
    remains like any other function call that one might perform.

    It is important to note that one cannot have RPCs having names that clash with other
    objects, or the names of other RPCs. This is due to the way the decorator is structured.
    Otherwise, there are little restrictions on what can an RPC do or not do.
    """
    # the endpoint of the function is the name of the function itself.
    endpoint = func.__name__.replace('_', '-')
    logging.debug(f"loading rpc call defintion for {endpoint}")

    signature = inspect.signature(func)

    # convert from an ordered dict to a normal dict
    # order of arguments does not matter as we will be passing in
    # all arguments as named arguments.
    arg_signature = dict(signature.parameters)
    # construct the named tuple
    arg_type = {k: v.annotation for k, v in arg_signature.items()}

    argument_name = f"{endpoint}_arguments"
    argument_serde_type = NamedTuple(argument_name, list(arg_type.items()))

    # register with sys modules to make sure that it is recognised
    try:
        _ = getattr(sys.modules[__name__], argument_name)

        # module found
        logging.warn(f"overwriting existing module at {argument_name}!"
                     " This is highly dangerous, and it is recommended that one ensures that "
                     "names do not conflict")
    except AttributeError:
        # module not found, safe to add to sys modules
        pass
    setattr(sys.modules[__name__], argument_name, argument_serde_type)

    # since Nonetype is not supported by mashumaro, we need to
    # disguise our solution in the form of bullshit. If we find an
    # str, I think we are in greater trouble.
    return_annotation = signature._return_annotation or None | str

    logging.debug(f"Initing RPC Call: {endpoint}({arg_type}) -> {return_annotation}")

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
        logging.debug(f"encoding {func_params} to {getattr(argument_serde_type, 'name')}")
        msg = msgpack_encode(list(func_params.values()), argument_serde_type)

        # Make the actual request
        assert __port in node.node.peers
        msg_content = node.node.send_message(target=__port, endpoint=endpoint, msg=msg)
        return_vals = msgpack_decode(msg_content, return_annotation) if msg_content is not None else None
        return return_vals

    def handler(msg: bytes) -> bytes:
        # deserialise incoming message
        logging.debug(f"Message to decode: {msg} -> {list(arg_type.items())} [{argument_serde_type}]")
        args = msgpack_decode(msg, argument_serde_type)
        # call function
        out = func(**args._asdict())
        logging.debug(f"Output from function: {out}")
        # return output
        return msgpack_encode(out, return_annotation)

    node.node.register_endpoint(endpoint, handler)

    return wrapper_func

