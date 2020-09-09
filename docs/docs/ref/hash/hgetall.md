# HGETALL key

Given the key of a hash, returns all contained pairs of fields and values.

*Cost:* One lookup + scan of _N_ items.

```
127.0.0.1:4445> hset myhash f1 v1
(integer) 1
127.0.0.1:4445> hset myhash f2 v1
(integer) 1
127.0.0.1:4445> hgetall myhash
1) "f1"
2) "v1"
3) "f2"
4) "v1"
```
