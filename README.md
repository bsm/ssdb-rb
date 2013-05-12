# ssdb-rb

A Ruby client library for [SSDB][ssdb-home], heaviliy inspired by the great
[redis-rb][redisrb-home] library. Requires SSDB version 1.4.2 or higher.

[ssdb-home]: https://github.com/ideawu/ssdb
[redisrb-home]: https://github.com/redis/redis-rb

### Installation

Install via rubygems:

```ruby
gem install ssdb
```

Use it with bundler, by adding the following line to your Gemfile:

```ruby
gem "ssdb"
```

For more information please visit http://gembundler.com/.

### Basic usage

Connect to SSDB, assuming it is listening on `localhost`, port 8888.

```ruby
require "ssdb"

ssdb = SSDB.new
```

To connect to a custom server, please provide a custom `:url` option:

```ruby
ssdb = SSDB.new url: "ssdb://1.2.3.4:8889"
```

To execute commands:

```ruby
ssdb.set("mykey", "hello world")
# => true

ssdb.get("mykey")
# => "hello world"
```

Full documentation of all commands is available on [rdoc.info][rdoc].

[rdoc]: http://rdoc.info/github/bsm/ssdb-rb/

### Batching/pipelining

Multiple commands can be executed as a batch operations. Instead of sending
commands one-by-one the client is able to send a batch of commands and
retrieve all responses as a single socket message exchange cycle.

Example:

```ruby
ssdb.batch do
  ssdb.set "foo", "5"
  ssdb.get "foo"
  ssdb.incr "foo"
end
# => [true, "5", 6]
```

### Futures

Results of individual batch operations are stored as *futures*. Future values
can be retrieved via the `#value` method once the batch execution is complete.

```ruby
ssdb.batch do
  v = ssdb.set "foo", "bar"
  w = ssdc.incr "baz"
end

v.value
# => true

w.value
# => 1
```

### TODO

* Implement HASH operations

### Licence (MIT)

```
Copyright (c) 2013 Black Square Media Ltd

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```