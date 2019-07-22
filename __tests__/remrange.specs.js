const { createClient, insertData, getRange } = require('./redis');

const TIMESERIES_KEY = 'ts:remove_range';

describe('remrange', () => {
  let client;

  const start_ts = 1488823384;
  const samples_count = 50;


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

  function getSize() {
    return callMethod('size')
  }

  async function getSpan() {
    const response = await callMethod('span');
    return [ parseInt(response[0], 10), parseInt(response[1], 10) ]
  }

  it('should remove data based on range', async () => {

    const data = [];
    for (let i = 0; i < samples_count; i ++) {
      data.push(i);
    }

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const mid = data.length / 2;
    const mid_ts = start_ts + mid;
    const end_ts = start_ts + data.length;

    const count = await callMethod('remrange', mid_ts, end_ts);

    const remaining = await getSize();
    expect(remaining).toEqual(count);

    const interval = await getSpan();
    expect(interval.length).toEqual(2);
    expect(interval[0]).toEqual(start_ts);
    expect(interval[1]).toEqual(start_ts + mid - 1);

  });


  it('should handle FILTER option', async () => {

    const data = [];
    for (let i = 0; i < samples_count; i ++) {
      data.push(i);
    }

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const mid = data.length / 2;
    const mid_ts = start_ts + mid;
    const end_ts = start_ts + data.length;

    const count = await callMethod('remrange', mid_ts, end_ts, 'FILTER', `value>${mid}`);
    const expectedCount = data.filter(x => x > mid).length;
    expect(count).toEqual(expectedCount);

    const response = await getRange(client, TIMESERIES_KEY, '-', '+');
    const actual = response.map(x => parseInt(x[1].value));
    const expected = data.filter(x => x <= mid);

    expect(actual).toEqual(expected);
  });

  it('should handle the LIMIT option', async () => {

    const data = [];
    for (let i = 0; i < samples_count; i ++) {
      data.push(i);
    }

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    const mid = data.length / 2;
    const mid_ts = start_ts + mid;
    const end_ts = start_ts + data.length;

    const count = await callMethod('remrange', mid_ts, end_ts, 'LIMIT', 0, 10);
    expect(count).toEqual(10);

    const size = await getSize();
    expect(size).toEqual(data.length - count);
  });

});
