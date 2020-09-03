# GET key

Retrieves the contents of a key containing a string. If the key does not exist,
the empty string is returned.

If the key holds a different type other than string already, an error is returned.

*Cost:* Two lookups.

```
127.0.0.1:4445> set key chickens
OK
127.0.0.1:4445> get key
"chickens"
127.0.0.1:4445> get othertype
(error) ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value
```
