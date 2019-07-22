const { createClient, parseObjectResponse } = require('./redis');

const TIMESERIES_KEY = 'ts:add';

describe('add', () => {
  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });


  function callMethod(name, ...args) {
    return client.timeseries(TIMESERIES_KEY, name, ...args);
  }

  function add(ts, value) {
    if (typeof (value) !== 'object') {
      value = { value }
    }
    const args = Object.entries(value).reduce((res, [key, val]) => res.concat(key, val), []);
    return client.timeseries(TIMESERIES_KEY, 'add', ts, ...args);
  }

  it('should add a value to the set', async () => {
    const data = { beers: 30 };
    await add( 3000, data ) ;

    const size = await callMethod('size');
    expect(size).toEqual(1);

    const response = await callMethod('get', 3000).then(parseObjectResponse);

    expect(response).toEqual(data);
  });


  it('should allow arbitrary data to be associated with a timestamp', async () => {

    const data = {
      bool: true,
      int: 12345,
      string: "bazinga",
      float: 123.456
    };
    await add(1000, data);

    const response = await callMethod('get', 1000).then(parseObjectResponse);

    // necessary because redis doesnt return float values from Lua, and nil values in a table
    const expected = { ...data, bool:1, float: data.float.toString()};

    expect(response).toEqual(expected);
  });

  it('should disallow duplicate values', async () => {
    await add( 1000, 20);
    await add( 1000, 20);
    const count = await client.zlexcount(TIMESERIES_KEY, '-', '+');
    expect(count).toEqual(1);
  });

  it('should throw on mismatched key/value count', async () => {
    await callMethod('add', 1000, "last_name")
        .catch(e => expect(e.message).toMatch(/Number of arguments must be even/));
  });

});
