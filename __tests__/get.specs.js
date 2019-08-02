const { createClient, parseObjectResponse } = require('./redis');

const TIMESERIES_KEY = 'ts:get';

describe('get', () => {
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

  function insertData(timestamp, data) {
    let values = data;
    if (typeof data == 'object') {
      values = Object.entries(data).reduce((res, [key, val]) => res.concat(key, val), []);
    } else {
      values = ['value', data]
    }
    return callMethod('add', timestamp, ...values);
  }

  function getValue(timestamp, ...args) {
    return callMethod('get', timestamp, ...args);
  }

  function getObjectValue(timestamp, ...args) {
    return getValue(timestamp, ...args).then(parseObjectResponse);
  }

  it('should return the value associated with a timestamp', async () => {
    await insertData(1005, 200);

    const value = await getValue(1005);

    expect(value).toEqual(['value', 200]);
  });

  it('should substitute "*" for the current server time"', async () => {
    await insertData('*', 200);

    const value = await getValue('*');

    expect(value).toEqual(['value', 200]);
  });

  it('should return all values if no keys are specified', async () => {
    const states = {
      active: 1,
      waiting: 2,
      error: 3,
      complete: 4
    };
    await insertData(1005, states);

    const received = await getObjectValue(1005);

    expect(received).toEqual(states);
  });

  it('should support LABELS', async () => {
    const states = {
      active: 1,
      waiting: 2,
      error: 3,
      complete: 4
    };
    await insertData(1005, states);

    const received = await getObjectValue(1005, 'LABELS', 'active', 'complete');
    const expected = {
      active: 1,
      complete: 4,
    };

    expect(received).toEqual(expected);
  });


  it('should support REDACT', async () => {
    const states = {
      active: 1,
      waiting: 2,
      error: 3,
      complete: 4
    };
    await insertData(1005, states);

    const received = await getObjectValue(1005, 'REDACT', 'active', 'complete');
    const expected = {
      waiting: 2,
      error: 3,
    };

    expect(received).toEqual(expected);
  });
});
