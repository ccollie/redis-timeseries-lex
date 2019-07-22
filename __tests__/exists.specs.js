const { createClient } = require('./redis');

const TIMESERIES_KEY = 'ts:exists';

describe('exists', () => {
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

  it('should return true if the timestamp exists', async () => {
    await callMethod('add', 1005, 'name', 'alice');

    const exists = await callMethod('exists', 1005);

    expect(exists).toBe(1);
  });

  it('should NOT return true for a non-existent timestamp', async () => {
    await callMethod('add', 1005, 'Number', 6789, 'letter', 'a');

    const exists = await callMethod('exists', 9999);

    expect(exists).toBe(0);
  });

});
