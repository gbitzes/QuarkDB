# HGET key field

Given the key of a hash, retrieves the contents found in the given field. If the
key, or corresponding field does not exist, the empty string is returned.

If the key holds a different type other than hash already, an error is returned.

*Cost:* Two lookups.

```
127.0.0.1:4445> hset myhash f1 v1
(integer) 1
127.0.0.1:4445> hset myhash f2 v2
(integer) 1
127.0.0.1:4445> hget myhash f1
"v1"
127.0.0.1:4445> hget myhash f2
"v2"
```
