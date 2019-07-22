const pAll = require("p-all");
const { createClient } = require('./redis');

const TIMESERIES_KEY = 'ts-test:size';

describe('size', () => {
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

  async function addValues(...args) {
    const values = [].concat(...args);
    const calls = [];
    for (let i = 0; i < values.length; i += 2) {
      const ts = values[i];
      const val = values[i+1];
      const call = () => client.timeseries(TIMESERIES_KEY, 'add', ts, 'value', val);
      calls.push(call);
    }

    return pAll(calls, { concurrency: 16 });
  }


  it('should return the correct list size', async () => {
    let size = await callMethod('size');
    expect(size).toEqual(0);

    await addValues(1005, 200);
    await addValues(1000, 10, 2000, 20, 3000, 30);
    size = await callMethod('size');
    expect(size).toBe(4);
  });

});
