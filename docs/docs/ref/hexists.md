# HEXISTS key field

Given the key of a hash, checks whether the specified field exists.

If the key holds a different type other than hash already, an error is returned.

```
127.0.0.1:4446> hset myhash f1 v1
(integer) 1
127.0.0.1:4446> hexists myhash f1
(integer) 1
127.0.0.1:4446> hexists myhash f2
(integer) 0
```