const { createClient } = require('./redis');
const pAll = require('p-all');

const TIMESERIES_KEY = 'timeseries:span';

describe('span', () => {
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

  it('should return the first and last timestamp', async () => {

    await addValues(1005, 200);
    await addValues( 1000, 10, 2000, 20, 7500, 30);
    await addValues(6007, 400);

    const actual = await callMethod('span');
    expect(actual).toStrictEqual([1000, 7500]);
  });

  it('should return the same timestamp for start and end if there is only one entry', async () => {

    await callMethod('add', 6007, 'value', 400);

    const actual = await callMethod('span');
    expect(actual).toStrictEqual([6007, 6007]);
  });

});
