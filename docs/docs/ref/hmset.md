# HMSET key [field value] [field value] ...

Given the key of a hash and N field-value pairs, sets the contents of the corresponding
fields to the given values.

*Cost:* One lookup + N * (One lookup + one writes) + one write.

```
127.0.0.1:4446> hmset myhash f1 v1 f2 v2
OK
127.0.0.1:4446> hget myhash f1
"v1"
127.0.0.1:4446> hget myhash f2
"v2"
```
