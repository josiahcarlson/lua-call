
from redis import Redis
from lua_call import function

conn = Redis()

function .return_args("""
return ARGV
""", conn)

function .call_return("""
return CALL.return_args({}, {1, 2, 3, ARGV})
""", conn)
