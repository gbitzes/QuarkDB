# HSET key field value

Given the key of a hash, sets the value of the corresponding field to the given
contents.

Return value: The number of newly created fields.

If the key holds a different type other than hash already, an error is returned.

*Cost:* Two lookups + two writes.

```
127.0.0.1:4445> hset myhash f1 v1
(integer) 1
127.0.0.1:4445> hset myhash f1 v2
(integer) 0
127.0.0.1:4445> hget myhash f1
"v2"
```
