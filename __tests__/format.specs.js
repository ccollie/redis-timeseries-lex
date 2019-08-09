
const { createClient, parseObjectResponse } = require('./redis');
const msgpack = require('msgpack');

const TIMESERIES_KEY = 'ts:format';

function toKeyValueList(hash) {
  return Object.entries(hash).reduce((res, [key, value]) => res.concat(key, (value == null) ? 'null' : value), []);
}

describe('format', () => {
  const start_ts = 1511885909;
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
    const args = toKeyValueList(value);
    return callMethod('add', ts, ...args);
  }

  const data = {
    boolVal: true,
    intVal: 12345,
    stringVal: "bazinga",
    floatVal: "123.456"
  };

  test('normal response' , async () => {
    await add(start_ts, data);
    const actual = await callMethod('get', start_ts).then(parseObjectResponse);
    const expected = { ...data, floatVal: data.floatVal.toString(), boolVal: data.boolVal ? 1 : 0};
    expect(actual).toEqual(expected);
  });

  test('json response' , async () => {
    await add(start_ts, data);
    const response = await callMethod('get', start_ts, 'FORMAT', 'json');
    expect(typeof response).toEqual('string');
    const actual = JSON.parse(response);
    expect(actual).toEqual(data);
  });


  test.skip('msgpack response' , async () => {
    await add(start_ts, data);
    const response = await callMethod('get', start_ts, 'FORMAT', 'msgpack');
    expect(typeof response).toEqual('string');
    const actual = msgpack.unpack( Buffer.from(response, 'utf-8') );
    const expected = msgpack.pack(data);
    expect(actual).toEqual(expected);
  });


});
