# Password authentication

## Configuration

Three configuration options control password authentication in QuarkDB. Please note that
passwords need to contain a minimum of 32 characters.

* __redis.password__: Ensures that clients need to prove they know this password
in order to be able to connect. This includes other QuarkDB nodes: All QuarkDB
nodes part of the same cluster must be configured with the same password,
otherwise they won't be able to communicate.

* __redis.password_file__: An alternative to the above, except the password is
read from the specified file -- permissions must be `400` (`r-----`). Note that, any whitespace at the end of the password file contents is completely ignored, including the ending new-line, if any.

  This means, the three following password files will, in fact, give identical
  passwords:

  ```
  $ cat file1
  pickles<space><space><space>\n\n
  ```

  ```
  $ cat file2
  pickles\r\n
  ```

  ```
  $ cat file3
  pickles
  ```

  This is to simplify the common case of just having a single line in the
  passwordfile, without having to worry about newlines and whitespace
  at the end, and being able to easily copy paste the password to a `redis-cli`
  terminal.

* __redis.require_password_for_localhost__: By default, the requirement for
password authentication is lifted for localhost clients. Set this option to
true to require authentication at all times.

## Usage

An unauthenticated client will receive an error message for all issued commands:

```
some-host:7777> set mykey myvalue
(error) NOAUTH Authentication required.
```

Two ways exist for a client to prove they know the password - by simply sending
the password over the wire, like official redis does, or a signing challenge.

### AUTH

Simply send the password over the wire towards the server, just like in official redis:

```
some-host:7777> auth some-password
OK
```

### Signing challenge

This method of authentication is only viable from scripts, not interactively.
It avoids sending the password over plaintext.

* The client asks the server to generate a signing challenge, providing 64 random
bytes:

  ```
  eoshome-i01:7777> HMAC-AUTH-GENERATE-CHALLENGE <... 64 random bytes ...>
  "<... long string to sign...>"
  ```

* The client then signs the server-provided string using HMAC EVP sha256 and
the password it knows about:

  ```
  eoshome-i01:7777> HMAC-AUTH-VALIDATE-CHALLENGE <... hmac signature ...>
  OK
  ```

* The server validates the signature, and if the signatures match, lets the
client through.
