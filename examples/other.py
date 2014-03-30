
from redis import Redis
from lua_call import function

conn = Redis()

function .call_external("""
return CALL.example.return_args({}, {4, 5, 6, ARGV})
""", conn)
