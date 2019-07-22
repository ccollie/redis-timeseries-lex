const { createClient, insertData} = require('./redis');

const TIMESERIES_KEY = 'timeseries:times';

describe('times', () => {
  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  async function getTimes(min, max) {
    const args = [];
    if (typeof min !== 'undefined') {
      args.push(min);
      if (typeof max !== 'undefined') {
        args.push(max)
      }
    }
    return client.timeseries(TIMESERIES_KEY, 'times', ...args);
  }

  async function generateData() {
    const start_ts = 1511885909;
    const data = [];
    const timestamps = [];

    for (let i = 0; i < 50; i++) {
      data.push(i);
      timestamps.push(start_ts + i);
    }

    await insertData(client, TIMESERIES_KEY, start_ts, data.length, data);

    return { start_ts, data, timestamps };
  }

  it('should return all the timestamps in the series', async () => {
    const { timestamps } = await generateData();
    let times = await getTimes();
    expect(times).toEqual(timestamps);
  });

  it('should return all the timestamps in a range', async () => {
    const { timestamps } = await generateData();
    const start = timestamps[5];
    const end = timestamps[21];
    const expected = timestamps.slice(5, 22);
    let times = await getTimes(start, end);
    expect(times).toEqual(expected);
  });

  it('should use the last timestamp as the max if not specified', async () => {
    const { timestamps } = await generateData();
    const start = timestamps[5];
    const expected = timestamps.slice(5);
    let times = await getTimes(start);
    expect(times).toEqual(expected);
  });

});
