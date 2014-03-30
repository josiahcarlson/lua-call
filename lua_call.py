
'''
lua_call - a Redis + Lua scripting namespace and calling library for Python

Copyright 2014 Josiah Carlson

This library released under the MIT license.

What is lua_call?
=================

This library implements a script transformation function along with some
useful utilities to allow you to call Lua scripts from other Lua scripts
in Redis. There are also some Python-side wrappers to aid in calling Lua from
Python, but that's just a bonus.

Generally speaking, it adds an internal calling semantic that allows you to
pass `KEYS` and `ARGV` between your internal Lua scripts in a sane manner.
This allows you, as a developer, to to develop your Lua scripts using better
practices than copying/pasting similar code between scripts.

Limitations
===========

Due to the way we handle calling conventions, you must be careful in how you
use `KEYS` and `ARGV`. Because we are not using a full-on parser for the Lua
language, we use a regular expression to discover uses of `KEYS` and `ARGV` to
alter. More specifically, we take examples like the following::

    local passed_keys = KEYS
    local source = KEYS[1]
    local arg = ARGV[1]
    local z = redis.call('KEYS', ...)

... and translate them into::

    local passed_keys = _KEYS
    local source = _KEYS[1]
    local arg = _ARGV[1]
    local z = redis.call('KEYS', ...)

Note that we didn't mangle the `KEYS` in `redis.call()`, but if you were to
have the following::

    local string = 'this is a string with KEYS and ARGV, oops!'

... we will mangle the string into::

    local string = 'this is a string with _KEYS and _ARGV, oops!'

There are other potential corner cases where our name mangling might be
incorrect, and you are advised to keep your usage of `KEYS` and `ARGV` to
reading or writing to `KEYS` and `ARGV` or to the simple literal strings of
`'KEYS'` and `'ARGV'` as in `redis.call('KEYS', ...)`.

Defining scripts using lua_call
===============================

You have a Redis connection during script definition
----------------------------------------------------

If you have your Redis connection available while defining your Lua scripts,
you can use the following calls to automatically define and register the
function wrappers in the Python module, automatically load the script into
Redis, and register the function for internal calling inside Redis::

    # contents of example.py
    from redis import Redis
    from lua_call import function

    conn = Redis(...)

    function .return_args("""
    return ARGV
    """, conn)

    function .call_return("""
    CALL.return_args({}, {1, 2, 3, ARGV})
    """, conn)

We describe how to use these functions just past the next section.

You don't have a Redis connection during script definition
----------------------------------------------------------

If you don't have a connection during script definition, you can omit the
connection argument during definition. In this case, the scripts will not be
registered, so you must later call `load_scripts()` to register them. The
following is more or less equivalent to the 'have a connection' section
above::

    # contents of example.py
    from redis import Redis
    from lua_call import function, load_scripts
    
    function .return_args("""
    return ARGV
    """)
    
    function .call_return("""
    CALL.return_args({}, {1, 2, 3, ARGV})
    """)
    
    load_scripts(Redis(), __name__)

Calling scripts defined with lua_call
=====================================

Assuming that you have defined your scripts using one of the two methods
outlined above, the `example` module will have functions defined in the module
namespace called `return_args()` and `call_return()`. These wrappers around
the script take exactly 3 arguments: a Redis connection, then a list of `KEYS`
and a list of `ARGV` that are passed to the called scripts.

An example of their use can be seen below::

    >>> from redis import Redis
    >>> conn = Redis()
    >>> import example
    >>> example.return_args(conn, [], [1, 2, 3])
    ['1', '2', '3']
    >>> example.call_return(conn, [], [4, 5, 6])
    [1, 2, 3, ['4', '5', '6']]

Note that while KEYS and ARGV passed from outside Redis are translated into
strings as part of the calling process, internal calls do not change the types
of arguments passed.

How it works
============

This library takes scripts that you define, possibly including other Lua
script calls, and changes the source code to allow you to actually perform
those calls. Generally speaking, you can think of this as introducing a new
global value in Redis by the name of `CALL`, which allows you to both register
functions and call those functions. Now, the truth is that there is no new
global value available in Redis Lua scripting, but your scripts will act as
though that is the case.

As an example of what actually goes on, let's say that we start out with a Lua
script defined as the below, which is from the included `example.py`::

    return CALL.return_args({}, {1, 2, 3, _ARGV})

After our transformation (and applying some source code formatting and extra
comments so you can understand what is going on easier), we get the following
script::

    -- We reference either the externally-called KEYS/ARGV or the internally
    -- called KEYS/ARGV in locals called _KEYS and _ARGV
    local _KEYS, _ARGV;
    if #ARGV == 0 or type(ARGV[#ARGV]) == 'string' then
        -- Use the standard KEYS and ARGV as passed from the external caller
        _KEYS = KEYS;
        _ARGV = ARGV;
    else
        -- Pull the KEYS and ARGV from the table appended to ARGV
        _KEYS = ARGV[#ARGV][1];
        _ARGV = ARGV[#ARGV][2];

        -- We remove the pushed reference to prevent circular references,
        -- which can crash Redis if you aren't careful
        table.remove(ARGV);
    end;

    -- push the arguments onto the ARGV table as call stack arguments
    table.insert(ARGV, {{}, {1, 2, 3, _ARGV}});

    -- fetch the script hash from the name and call the function
    return _G[redis.call('HGET', ':registry', 'example.return_args')]();

Generally, there is some header code prepended to your source, KEYS and ARGV
references are changed to _KEYS and _ARGV, and any time you want to make a
call to another script, we append your arguments to the end of the ARGV table,
and pull the destination script name from a Redis-backed function registry.

Early versions of this library required assigning the result of a call to a
local variable before returning, but this is no longer necessary.

Licensing and source code mangling
==================================

Technically speaking, this library will alter the Lua script source code that
you pass in order to insert the code that handles internal calls. I do not
consider this purposeful alteration to result in your code being in any way
derived from or related to this library. Your source code remains your source
code, and this library is a utility to aid in your development and maintenance
processes.

'''

from hashlib import sha1
import re
import sys

__all__ = ['dummy', 'function', 'load_scripts']

VERSION = '0.10.0'

REGISTRY = {}

#------------------------ our dummy Redis connection -------------------------

@apply
class dummy(object):
    '''A fake Redis connection you can use to defer script registration.'''
    def __getattr__(self, name):
        return self
    def __call__(self, *args, **kwargs):
        return self

#------------------ This handles the script fixing/mangling ------------------

# This handles/fixes arguments to be consistent whether you are calling from
# outside or inside Redis.
CALL_HEADER = '''
local _KEYS, _ARGV;
if #ARGV == 0 or type(ARGV[#ARGV]) == 'string' then
    _KEYS = KEYS;
    _ARGV = ARGV;
else
    _KEYS = ARGV[#ARGV][1];
    _ARGV = ARGV[#ARGV][2];
    table.remove(ARGV);
end;'''.replace('\r', '').replace('\n', ' ')

# We need to mangle our KEYS and ARGV references because we are using locals
# for it, as we are using the globals for alternate argument passing
KEYS_RE = re.compile('(?<!["\'])((?:KEYS)|(?:ARGV))(?!["\'])')

# Function calls is where we really start to gain some benefit. We're going to
# use the ARGV table to carry the KEYS and ARGV to the called function, with
# the header that we inserted above automatically handling external/internal
# calls and unpacking our passed KEYS and ARGV.
CALL_RE = re.compile(
    '^(.*?)CALL[.]([a-zA-Z_][a-zA-Z0-9_]*(?:[.][a-zA-Z_][a-zA-Z0-9_]*)*)[(]([^()]+)[)]\s*?$', re.MULTILINE)
PUSH_STACK_CALL_FCN = '''table.insert(ARGV, {%s});%s_G[redis.call('HGET', ':registry', '%s')]();'''

def _fix_calls(script, module):
    '''
    This injects are function call handling header, fixes references to KEYS
    and ARGV, and translates "Y = CALL.<name>(...)" function calls into the
    Lua code necessary to make it all happen.

    You shouldn't need to call this directly, but you can use this to
    transform your Lua scripts if you don't want to use the rest of the
    module.
    '''
    script = KEYS_RE.sub(r'_\1', script)
    def fix_calls(match):
        left, full, args = match.groups()
        if '.' not in full:
            full = (module + '.' + full).lstrip('.')
        return PUSH_STACK_CALL_FCN%(args, left, full)
    
    script = CALL_RE.sub(fix_calls, script)
    return CALL_HEADER + script

#------------------ This handles the function registration -------------------

def load_scripts(conn, prefix=''):
    '''
    When given a Redis connection, this will register all known scripts in
    Redis with their names included for subsequent calling. If you passed a
    non-dummy Redis connection in your `function .fcn_name(..., conn)`
    registry call, then you shouldn't need to call this.

    If you only want to load a subset of known scripts, you can pass an
    optional prefix to load. As an example, say that you have a module defined
    like::

        from lw import dummy, function, load_scripts

        function .new('return 1', dummy)

        def register(conn):
            load_scripts(conn, __name__)

    By calling the `register()` function you have defined with the Redis
    connection, you will *only* register those scripts that were defined in
    the module or submodules.
    '''
    prefix = '' if prefix == '__main__' else prefix
    prefix = (prefix.rstrip('.') + '.') if prefix else ''
    pipe = conn.pipeline(True)
    added = 0
    for full, (hash, fixed) in REGISTRY.iteritems():
        if full.startswith(prefix):
            pipe.script_load(fixed)
            pipe.hset(':registry', full, hash)
            added += 1
    pipe.execute()
    return added

@apply
class function(object):
    '''
    Use me to register your Lua functions like::

        function .script_name('script contents', conn)

    This tool will perform several operations:
    
    * Transform your Lua script to allow for internal calls from other scripts
    * Transform your Lua script to make any calls according to the rules
      outlined in the documentation
    * Register the wrapped Lua script in the globals of your calling module
      to allow for easy calling
    * If a valid Redis connection is passed, this will also register the
      function inside Redis to be available for calling at any time

    .. Note: omitting the Redis connection requires a later
      `load_scipts(conn)` call

    '''
    def __getattr__(self, name):
        return _register(name)

def _register(name):
    '''Internal implementation detail, you can ignore me.'''
    if '.' in name:
        raise ValueError("Cannot define a function name with an included period")
    def call(script, conn=dummy):
        gl = sys._getframe(1).f_globals
        module = gl['__name__']
        if module == '__main__':
            module = ''
        full = (module + '.' + name).lstrip('.')

        # fix/mangle the script
        fixed = _fix_calls(script, module)
        hash = sha1(fixed).hexdigest()

        # register the function in Redis
        hash2 = conn.pipeline(True) \
            .script_load(fixed) \
            .hset(':registry', full, 'f_' + hash) \
            .execute()
        # keep a local copy
        REGISTRY[full] = ('f_' + hash, fixed)
        # register the name in the namespace
        gl[name] = f = _caller(hash)
        return f
    return call

def _caller(hash):
    '''Internal implementation detail, you can ignore me.'''
    def call(conn, keys, argv):
        return conn.evalsha(hash, len(keys), *(list(keys) + list(argv)))
    return call
