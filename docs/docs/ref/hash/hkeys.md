# HKEYS key

Given the key of a hash, returns the names of all stored fields.

*Cost:* One lookup + scan of _N_ items.

```
127.0.0.1:4446> hset myhash f1 v1
(integer) 1
127.0.0.1:4446> hset myhash f2 v1
(integer) 1
127.0.0.1:4446> hkeys myhash
1) "f1"
2) "f2"
```
