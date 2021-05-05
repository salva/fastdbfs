import shlex
import re
import os.path
import posixpath
import logging
import humanfriendly
import dateparser
import fnmatch

def _wrap(func):
    if hasattr(func, "arg_decls"):
        return func

    func_name = func.__name__

    def wrapper(self, cmd_line):
        kwargs = _parse_args(wrapper, cmd_line)
        logging.debug(f"Calling function {func_name} with arguments {kwargs}")
        return func(self, **kwargs)

    wrapper.__name__=func_name
    wrapper.__doc__=func.__doc__
    wrapper.arg_decls=[]
    return wrapper

def _normalize_key(name):
    return re.sub(r'\W', '_', name)

class _ArgDecl:
    def __init__(self, option=False, arity="1", default=None, preprocess=None,
                 cast=None, cast_args={}, **kwargs):
        self.option=option
        if option:
            self.names=kwargs["names"]
            self.name=self.names[0]
        else:
            self.name=kwargs["name"]
            self.names = [self.name]
        self.key=_normalize_key(self.name)
        self.arity=arity
        self.default=default
        self.cast=cast
        self.cast_args=cast_args
        self.preprocess=preprocess
        self.kwargs=kwargs

    def __str__(self):
        return f"ArgDecl({self.key}, names: {self.names}, option: {self.option}, arity: {self.arity}, default: {self.default})"

    def __repr__(self):
        return self.__str__()

def _add_arg_decl(func, **kwargs):
    wrapper = _wrap(func)
    arg_decl = _ArgDecl(**kwargs)
    wrapper.arg_decls.insert(0, arg_decl)
    return wrapper

def _option_lookup(option_decls, name):
    for decl in option_decls:
        for decl_name in decl.names:
            if decl_name == name:
                return decl
    raise Exception(f"Invalid option {name}")

def _compile_regex(str, case_insensitive):
    logging.debug(f"compiling pattern {str}, case_insensitive: {case_insensitive}")
    return re.compile(str, 0 if case_insensitive else re.I)

def _cast(arg, type, extra):
    # print(f"converting {arg} to {type}")
    if type == "int":
        return int(arg)
    if type == "size":
        return humanfriendly.parse_size(arg)
    if type == "glob":
        ci = extra.get("case_insensitive", False)
        return _compile_regex(fnmatch.translate(arg), ci)
    if type == "re":
        ci = extra.get("case_insensitive", False)
        return _compile_regex(arg, ci)
    if type.startswith("date"):
        settings = {"PREFER_DATES_FROM": "past"}
        if type == "date>":
            pass
        elif type == "date<":
            pass
        elif type == "date":
            pass
        else:
            raise Exception(f"Internal error: Invalid cast type {type}")
        dt = dateparser.parse(arg, settings=settings)
        # print(f"time limit for {type} {arg} is {dt} ({dt.timestamp()})")
        return dt.timestamp()
    if type == "date>":
        dt
    raise Exception(f"Internal error: Invalid cast type {type}")

def _parse_args(wrapper, cmdline):
    option_decls = []
    list_decls = []

    for arg_decl in wrapper.arg_decls:
        # print(arg_decl)
        if arg_decl.option:
            option_decls.append(arg_decl)
        else:
            list_decls.append(arg_decl)

    args = shlex.split(cmdline)
    kwargs = {}
    no_more_options = False
    while args:
        current = args.pop(0)
        if current.startswith("-") and not no_more_options:
            value = None
            if current == "--" or current == "-":
                no_more_options=True
                continue

            if current.startswith("--"):
                single_dash = False
                try:
                    offset=current[3:].index("=")+3
                    name=current[2:offset]
                    value=current[(offset+1):]
                except:
                    name=current[2:]
            else:
                single_dash = True
                name = current[1]
                if len(current) > 2:
                    value = current[2:]
            decl = _option_lookup(option_decls, name)
            if decl.arity == "0":
                kwargs[decl.key] = True
                if value is not None:
                    if single_dash:
                        # we allow the user to pack several options
                        # into one. For instance:
                        #     -lptfoo ==> -l -p -tfoo
                        args.insert(0, "-" + value)
                    else:
                        raise Exception(f"Option {name} doesn't take a value")
            else:
                if value is None:
                    if not args:
                        raise Exception(f"Option {name} requires a value")
                    value = args.pop(0)

                if decl.arity in ("1", "?"):
                    # we unwrap single values below.
                    kwargs[decl.key] = [value]
                elif decl.arity in ("*", "+"):
                    kwargs.setdefault(decl.key, []).append(value)
                else:
                    raise Exception(f"Internal error: unexpected arity {decl.arity} for {decl.key}")

        else: # so, it is not an option...
            for decl in list_decls:
                if decl.arity in ("*", "+"):
                    kwargs.setdefault(decl.key, []).append(current)
                    break
                if decl.key not in kwargs:
                    # we push an array anyway, even when the arity is
                    # 1 or ? in order to simplify the stealing logic
                    # below...

                    kwargs[decl.key]=[current]
                    break
            else:
                raise Exception(f"Too many arguments")

    # Steal from previous arguments in order to comply with arity
    # restrictions. We bubble up the arguments here!
    done = False
    while not done:
        done = True
        src = None
        surplus = 0
        for decl in list_decls:
            args = kwargs.get(decl.key, [])
            if args:
                if len(args) > 1:
                    src = decl
                    surplus = len(args) - 1
                else:
                    src = decl
            elif surplus > 0:
                kwargs[decl.key] = [kwargs[src.key].pop()]
                src = decl
                surplus -= 1
                done = False
            else:
                break

    # Now, we set the defaults, check arities and unwrap one value arguments and preprocess
    for decl in wrapper.arg_decls:
        args = kwargs.get(decl.key, None)
        if decl.arity in ("1", "?"):
            arg = args[0] if args else decl.default
            if decl.arity == "1" and arg is None:
                raise Exception(f"Missing mandatory argument {decl.name}")
            if decl.cast and arg is not None:
                arg = _cast(arg, decl.cast, decl.cast_args)
            if decl.preprocess:
                arg = decl.preprocess(arg)
            kwargs[decl.key] = arg
        else:
            if not args:
                args = decl.default
                if not args and decl.arity == "+":
                    raise Exception(f"Missing mandatory argument {decl.name}")
                if decl.preprocess:
                    args = [decl.preprocess(arg) for arg in args]
                kwargs[decl.key] = args

    return kwargs


# And now, finally, the decorators!!!

def option(*names, arity="?", **kwargs):
    def decorator(f):
        return _add_arg_decl(f, names=names, option=True, arity=arity, **kwargs)
    return decorator

def flag(*names, default=False, **kwargs):
    def decorator(f):
        return _add_arg_decl(f, names=names, option=True, arity="0", default=default, **kwargs)
    return decorator

def arg(name, arity="1", **kwargs):
    def decorator(f):
        return _add_arg_decl(f, name=name, arity=arity, **kwargs)
    return decorator

def remote(name, arity="1", **kwargs):
    def decorator(f):
        return _add_arg_decl(f, name=name, arity=arity, **kwargs)
    return decorator

def _expanduser(path):
    if path is None:
        return None
    return os.path.expanduser(path)

def local(name, arity="1", **kwargs):
    def decorator(f):
        return _add_arg_decl(f, name=name, arity=arity,
                             preprocess=_expanduser,
                             **kwargs)
    return decorator

def argless():
    def decorator(f):
        wrapper = _wrap(f)
        return wrapper
    return decorator

def chain(*decorators):
    def decorator(f):
        for d in decorators:
            f = d(f)
        return f
    return decorator
