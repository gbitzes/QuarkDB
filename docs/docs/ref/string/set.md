# SET key value

Stores the given value onto the specified key. If the key does not exist, it is
created.

If the key holds a different type other than string already, an error is returned.

*Cost:* One lookup + two writes.

```
127.0.0.1:4445> set key chickens
OK
127.0.0.1:4445> get key
"chickens"
127.0.0.1:4445> set othertype b
(error) ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value
```
