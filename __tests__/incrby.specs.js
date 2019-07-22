const { createClient, parseObjectResponse } = require('./redis');

const TIMESERIES_KEY = 'ts:incrBy';

describe('incrBy', () => {
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

  function incrBy(timestamp, valueHash) {
    const args = Object.entries(valueHash).reduce((res, [key, value]) => res.concat(key, value), []);
    return callMethod('incrBy', timestamp, ...args);
  }

  function add(ts, value) {
    if (typeof (value) !== 'object') {
      value = { value }
    }
    const args = Object.entries(value).reduce((res, [key, val]) => res.concat(key, val), []);
    return callMethod('add', ts, ...args);
  }

  it('should create the value if it does not exist' , async () => {

    const data = {
      active: 1,
      waiting: 2,
    };

    await incrBy(start_ts, data);
    const actual = await getValue(start_ts);
    expect(actual).toEqual(data);
  });


  it('should increment the value for a hash key' , async () => {

    const data = {
      active: 1,
      waiting: 2,
      completed: 3,
      failed: 4,
    };

    const increments = {
      active:  4,
      waiting: 3,
      completed: 2,
      failed: 1
    };

    const expected = {
      active:  5,
      waiting: 5,
      completed: 5,
      failed: 5
    };

    await add(start_ts, data);

    await incrBy(start_ts, increments);

    const actual = await getValue(start_ts);

    expect(actual).toEqual(expected);
  });


  it('should return the post increment values' , async () => {

    const data = {
      active: 1,
      waiting: 2,
      completed: 3,
      failed: 4,
    };

    await add(start_ts, data);

    const actual = await callMethod('incrBy', start_ts, 'active', 2, 'failed', 1);

    expect(actual).toEqual([3, 5]);
  });


  it('should preserve float values' , async () => {

    const data = {
      active: 1,
      waiting: 2,
      completed: 3,
      failed: 4,
    };

    await add(start_ts, data);

    const actual = await callMethod('incrBy', start_ts, 'active', 2.5, 'failed', 1.5);

    expect(actual).toEqual(["3.5", "5.5"]);
  });
});
