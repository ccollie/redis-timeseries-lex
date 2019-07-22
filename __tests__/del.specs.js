
const { createClient, insertData, getRange } = require('./redis');

const TIMESERIES_KEY = 'add_test_key';

describe('del', () => {
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


  function del(...keys) {
    const args = [].concat(...keys);
    return callMethod('del', ...args)
  }


  it('should add a delete a value from the set', async () => {
    const id = await callMethod('add', 1000, "beers", 30);
    const count = await del(id);
    expect(count).toEqual(1);
  });


  it('should allow a variable amount of keys', async () => {
    const start_ts = 1488823384;
    const samples_count = 20;

    const data = [];

    for (let i = 0; i < samples_count; i++) {
      data.push( i );
    }

    await insertData(client, TIMESERIES_KEY, start_ts, samples_count, data);
    const items = await getRange(client, TIMESERIES_KEY, '-', '+');
    const ids = items.map(x => x[0]);

    const count = await del(ids);
    expect(count).toEqual(ids.length);

  });

});
