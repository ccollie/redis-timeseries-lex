
const { createClient, parseObjectResponse } = require('./redis');


const TIMESERIES_KEY = 'ts:set';

function toKeyValueList(hash) {
  return Object.entries(hash).reduce((res, [key, value]) => res.concat(key, value), []);
}

describe('set', () => {
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

  function getValue(timestamp) {
    return callMethod('get', timestamp).then(parseObjectResponse);
  }

  function set(timestamp, valueHash) {
    const args = toKeyValueList(valueHash);
    const res = callMethod('set', timestamp, ...args);
    return res;
  }

  function add(ts, value) {
    if (typeof (value) !== 'object') {
      value = { value }
    }
    const args = toKeyValueList(value);
    return callMethod('add', ts, ...args);
  }

  it('should create the value if it does not exist' , async () => {

    const data = {
      active: 1,
      waiting: 2,
    };

    await set(start_ts, data);
    const actual = await getValue(start_ts);
    expect(actual).toEqual(data);
  });


  it('should set the values' , async () => {

    const data = {
      active: 1,
      waiting: 2,
      completed: 3,
      failed: 4,
    };

    const newValues  = {
      active:  4,
      waiting: 3,
      completed: 2,
      failed: 1
    };

    await add(start_ts, data);

    await set(start_ts, newValues);

    const actual = await getValue(start_ts);
    expect(actual).toEqual(newValues);

  });

});
