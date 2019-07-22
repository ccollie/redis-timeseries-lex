const { createClient, addValues } = require('./redis');

const TIMESERIES_KEY = 'ts:count';

describe('count', () => {
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


  it('should return the count of elements between 2 timestamps', async () => {
    await addValues(client, TIMESERIES_KEY, 1000, 10, 2000, 20, 3000, 30, 4000, 40, 5000, 50, 6000, 60);
    const count = await callMethod('count', 2000, 5000);
    expect(count).toEqual(4);
  });

  it('supports special range characters', async () => {
    await addValues( client, TIMESERIES_KEY, 1000, 10, 2000, 20, 3000, 30, 4000, 40, 5000, 50, 6000, 60, 7000, 60);

    let count = await callMethod('count', '-', '+');
    expect(count).toEqual(7);

    count = await callMethod('count', 3000, '+');
    expect(count).toEqual(5);

    count = await callMethod('count', '-', 4000);
    expect(count).toEqual(4);
  });

  it('should support FILTER', async () => {
    await addValues(client, TIMESERIES_KEY, 1000, 10, 2000, 20, 3000, 30, 4000, 40, 5000, 50, 6000, 60);
    const count = await callMethod('count', 1000, 5000, 'FILTER', 'value>30');
    expect(count).toEqual(2);
  });

});
