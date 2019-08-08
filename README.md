Redis Timeseries Lex
====================

Redis Timeseries is a Lua library implementing queryable time series on top of Redis using
[lexicographically sorted sets](https://redislabs.com/redis-best-practices/time-series/lexicographic-sorted-set-time-series/).

##### Encoding
Each data point is stored as a ZSET entry with score 0 and value in the form `timestamp|value`, 
where `value` is a set of arbitrary key value pairs encoded using message pack. The encoding is to preserve space, as
well as to preserve numeric values.

Because of Lua => Redis conversion issues ( [float truncation](https://redis.io/commands/eval#conversion-between-lua-and-redis-data-types) specifically )
we try to preserve float values by converting them to string as necessary on return from command calls.


#### Testing

A set of tests (using `node/jest`) are available in `__tests__`. To run these,
you will need `node`. If you have `node` installed:

```bash
npm install
npm run test
```

If you have Redis running somewhere other than `localhost:6379`, you can supply
the `REDIS_URL` environment variable:

```bash
REDIS_URL='redis://host:port' npm run test
```

#### Structure
We recommend using
[git submodules](http://git-scm.com/book/en/Git-Tools-Submodules) to keep
[redis-timeseries-lex](https://github.com/ccollie/redis-timeseries-lex) in your bindings.

#### Usage
Load the script into redis, using [SCRIPT LOAD](https://redis.io/commands/script-load), or better yet,
use any of the redis client libraries available for the programming language of your choice. The docs
assumes that you have successfully done so and have acquired the associated `sha`. For all implemented 
commands, the script takes a single key representing the timeseries. The general format of calls is 
as follows 


```bash
evalsha sha 1 key command [args1 ...]
```

For example

```bash
evalsha b91594bd37521... 1 wait-time:5s range 1548149180000 1548149280000 AGGREGATION max 5000 
```


### Commands

#### add <a name="command-add"></a>
Add one or more data points to the timeseries at `key`. An abitrary number of key-value pairs can be specified.

```bash
evalsha sha 1 key timestamp [key value ...] 
```
Values which can be interpreted as numbers will be stored and treated as such.

Example

```bash
evalsha b91594bd37521... 1 readings:temp add 1564632000000 temperature 18 altitude 500 geohash gbsvh tag important
```

#### get <a name="command-get"></a>
Get the value of a key associated with *timestamp*

```bash
evalsha sha 1 key get timestamp hash_label [LABELS field ...] [REDACT field ...]
```

##### Return Value
[Array reply](https://redis.io/topics/protocol#array-reply), JSON or msgpack depending on FORMAT option

The command returns the key-value data associated with a timestamp.  The returned order of fields and values is unspecified.

Examples

<pre><code>
redis-cli&gt; evalsha b91594bd37521 1 purchases add 1564632000000 item_id cat-987H1 cust_id 9A12YK2 amount 2500
1564632000000
redis-cli&gt; evalsha b91594bd37521 1 purchases get 1564632000000
1564632002100
1) "item_id"
2) "cat-987H1"
3) "cust_id"
4) "9A12YK2"
5) "amount"
6) "2500"
</code></pre>

The [LABELS](#labels-a-nameoption-labelsa) and [REDACT](#redact-a-nameoption-redacta) options can be used
to specify which fields are returned

<pre><code>
redis-cli&gt; evalsha b91594bd37521 1 purchases add 1564632000000 item_id cat-987H1 cust_id 9A12YK2 amount 2500
1564632000000
redis-cli&gt; evalsha b91594bd37521 1 purchases get 1564632000000 LABELS item_id amount
1564632002100
1) "item_id"
2) "cat-987H1"
5) "amount"
6) "2500"
redis-cli&gt; evalsha b91594bd37521 1 purchases get 1564632000000 REDACT cust_id
1564632002100
1) "item_id"
2) "cat-987H1"
5) "amount"
6) "2500"
</code></pre>

#### del <a name="command-del"></a>
Removes the specified members from the series stored at the given timestamp. 
Non existing members are ignored. Returns the number of deleted entries.

```bash
evalsha sha 1 key del [timestamp ...]
```

Example

```bash
evalsha b91594bd37521... 1 temperature del 1564632000000 1564632010000
```

##### Return Value
[Integer reply](https://redis.io/topics/protocol#integer-reply): the number of entries actually deleted.


#### pop <a name="command-pop"></a>
Remove and return a value at a specified timestamp 

```bash
evalsha sha 1 key pop timestamp [LABELS label ...] [REDACT label ...]
```

Example

```bash
evalsha b91594bd37521... 1 temperature pop 1564632000000
```

#### size <a name="command-size"></a>
Returns the number of items in the timeseries

```bash
evalsha sha 1 key size 
```

##### Return Value
[Integer reply](https://redis.io/topics/protocol#integer-reply): the number of items in the series.

#### count <a name="command-count"></a>
Returns the number of items in the timeseries between 2 timestamps. The range is inclusive.
Note that this and all other commands expecting a range also accept the special characters `-` and `+`
to specify the lowest and highest timestamps respectively.

```bash
evalsha sha 1 key count min max [FILTER condition ...]
```

Example
```bash
evalsha b91594bd37521... 1 temperature count 1564632000000 1564635600000
```
##### Return Value
[Integer reply](https://redis.io/topics/protocol#integer-reply): the count of items in the subset defined by the given range and filter.

#### exists <a name="command-exists"></a>
Checks for the existence of a timestamp in the timeseries. 

```bash
evalsha sha 1 key exists timestamp
```

Example
```bash
evalsha b91594bd37521... 1 temperature exists 1564632000000
```

##### Return Value
[Integer reply](https://redis.io/topics/protocol#integer-reply): `1` if the timestamp exists, `0` otherwise.


#### span <a name="command-span"></a>
Returns a 2 element array containing the highest and lowest timestamp in the series.

```bash
evalsha sha 1 key span
```

#### times <a name="command-times"></a>
Returns a list of timestamps between *min* and *max*

```bash
evalsha sha 1 key min max
```


#### set <a name="command-set"></a>
Set the value associated with *timestamp*.

```bash
evalsha sha 1 key set timestamp hash_label1 value [hash_label2 value ...]
```

```bash
evalsha b91594bd37521... 1 rainfall set 1564632000000 geohash gbsut inches 2.0
```

#### incrBy <a name="command-incrby"></a>
Increment the values of fields associated with *timestamp*.

```bash
evalsha sha 1 key incrBy timestamp hash_label1 value [hash_label2 value ...]
```
example

```bash
evalsha b91594bd37521... 1 wait_time incrBy 1564632000000 count 1 value 2000
```

## Querying <a name="querying"></a>
### range/revrange/poprange <a name="command-range"></a>
Query a timeseries by range and optionally aggregate the result. *`revrange`* returns the range of members 
in reverse order of timestamp, but is otherwise to its non-reversed counterpart. `poprange` functions similarly to `range`,
but removes the data in the range before returning it.

```bash
evalsha sha 1 key [range|revrange|poprange] min max [FILTER condition ....] [AGGREGATION aggKey timeBucket] [LABELS label ....] [REDACT field ...] [FORMAT [json|msgpack]]
```

- `key` the timeseries redis key
- `min` the minimum timestamp value. The special character `-` can be used to specify the smallest timestamp
- `max` the maximum timestamp value. The special character `+` can be used to specify the largest timestamp

`min` and `max` specify an inclusive range.

#### Options <a name="options"></a>

##### FILTER <a name="option-filter"></a>

The `range` commands support a list of query filters.  

- `field=value` field equals value 
- `field!=value` field is not equal to value 
- `field>value` field greater than value 
- `field>=value` field greater than or equal value 
- `field<value` field less than value 
- `field<=value` field less than or equal value 
- `field=null` value is null or the field `field` does not exist in the data
- `field!=null` the field has a value 
- `field=(v1,v2, ...)` `field` equals one of the values in the list 
- `field!=(v1,v2, ...)` `field` value does not exist in the list. 

Any number of filter conditions can be provided, and the results are `ANDed` by default, however you may use an `OR`
if required.

```
FILTER purchase_price>5000 OR customer_status=preferred
```

For contains comparisons, avoid leading and trailing spaces with numbers. In other words use

```
FILTER size!=(10,20,30)
```

instead of

```
FILTER size!=(10 , 20, 30)
```

The parser supports quoted as well as non quoted strings e.g.

```
FILTER state!=(ready,"almost done",burnt to a crisp)
``` 

String as well as numeric comparisons are supported.

```
evalsha b91594bd37521... 1 game_scores range - + FILTER tag=playoffs value>10
```


##### AGGREGATION <a name="option-aggregation"></a>

A timeseries range can be rolled up into buckets and aggregated by means of the AGGREGATION option :

 ```
 evalsha sha 1 key range_command min max AGGREGATION timebucket aggegation field [ aggregation field ... ]
 ```

- `timeBucket` - time bucket for aggregation. The units here should be the same as used when adding data.
- `aggregation` - *avg, sum, min, max, range, count, first, last, stats, distinct, count_distinct*
- `field` - the field to aggregate

| Aggregation    | Description                                   |
|:---------------|:----------------------------------------------|
| first          | the first valid data value                    |
| last           | the last valid data value                     |
| min            | minimum data value                            |
| max            | maximum data value                            |
| avg            | mean of values in time range                  |
| count          | the number of data points                     |
| range          | the difference between the max and min values |
| sum            | the sum of values                             |
| distinct       | the list of unique values in the range        |
| distinct_count | the count of unique values in the range       |

Example

```
evalsha b91594bd37521...  1 temperature:3:32 range 1548149180000 1548149210000 AGGREGATION 5000 avg value
```

For `range` and `revrange`, each key will be aggregated as appropriate, subject to any supplied `LABELS`.
In this case, the query will return a pair of `[timestamp, object]` where the values of `object` are the aggregated
values for the appropriate keys in the given *min*/*max* range.

##### LABELS <a name="option-labels"></a>

The `LABELS` option may be specified to limit the fields returned from a query or available for aggregation. If this
option is specified, only the mentioned fields are returned from queries (opt-in).

```
evalsha b91594bd37521... 1 temperatures range - + FILTER temperature>=45 LABELS name lat long
```

##### REDACT <a name="option-redact"></a>

`REDACT` is used to specify fields which should NOT be returned from a query or made available for aggregation. All
fields not explicitly specified are returned (opt-out)

```
evalsha b91594bd37521... 1 purchases range - + FILTER amount>=5000 REDACT credit_card_number
```


##### FORMAT <a name="option-format"></a>

Selects the output format of query. Currently `json` and `msgpack` are supported. If not specified, 
range and aggregation responses are sent using a redis multi-bulk reply.

```
evalsha b91594bd37521... 1 purchases range - + FILTER amount>=5000 FORMAT json
```

### copy <a name="command-copy"></a>
Executes a `range` and copies the result to another key.

```bash
evalsha sha 2 src dest min max [FILTER condition ....] [AGGREGATION timeBucket aggregate field aggregate field ...] [LABELS label ....] [REDACT field ...] [STORAGE ["timeseries"|"hash"]]
```

- `key` the timeseries redis key
- `min` the minimum timestamp value. The special character `-` can be used to specify the smallest timestamp
- `max` the maximum timestamp value. The special character `+` can be used to specify the largest timestamp

`min` and `max` specify an inclusive range.

#### Options <a name="options"></a>
All options for `range` are accepted with the exception of `FORMAT`. In addition we may specify a `STORAGE` option

- `timeseries` (default) store results in a timeseries sorted set
- `hash` stores the result in a hash where the key is the timestamp