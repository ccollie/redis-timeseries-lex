const pAll = require('p-all');
const { createClient, getRange, insertData } = require('./redis');

const TIMESERIES_KEY = 'ts:aggregation';

describe('aggregation', () => {

  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  // aggregation

  async function insertAggregationData(key = TIMESERIES_KEY) {
    const calls = [];
    const data = [];

    const values = [31, 41, 59, 26, 53, 58, 97, 93, 23, 84];
    for (let i = 10; i < 50; i++) {
      const val = Math.floor(i / 10) * 100 + values[i % 10];
      data.push([i, val]);
      calls.push( () => client.timeseries(key, 'add', i, 'value', val) );
    }

    await pAll(calls, { concurrency: 8 });

    return data;
  }

  async function runAggregation(key, min, max, aggType) {
    const response = await getRange(client, key, min, max, 'AGGREGATION', aggType, 10);
    const actual = response.map( ([ts, data]) => {
      return [ts, parseFloat(data.value)]
    });
    return actual;
  }

  async function testAggregation(type, expected) {
    await insertAggregationData(TIMESERIES_KEY);
    const actual = await runAggregation(TIMESERIES_KEY, 10, 50, type);
    expect(actual).toEqual(expected)
  }

  function calc_stats(values) {
    let count = 0;
    let sum = 0;
    let max = Number.NEGATIVE_INFINITY;
    let min = Number.POSITIVE_INFINITY;
    let vk = 0;
    let mean = 0;
    let std = 0;

    values.forEach(v => {
      let val = parseFloat(v);
      if (!isNaN(val)) {
        let oldMean = mean;
        count = count + 1;
        sum = sum + val;
        max = Math.max(max, val);
        min = Math.min(min, val);
        mean = sum / count;
        vk = vk + (val - mean) * (val - oldMean);
        std = Math.sqrt(vk / (count - 1));
      }
    });
    return {
      count,
      sum,
      min,
      max,
      mean,
      std
    }
  }

  test('min', async () => {
    const expected = [[10, 123], [20, 223], [30, 323], [40, 423]];
    await testAggregation('min', expected);
  });


  test('max', async () => {
    const expected  = [[10, 197], [20, 297], [30, 397], [40, 497]];
    await testAggregation('max', expected);
  });

  test('avg', async () => {
    const expected = [[10, 156.5], [20, 256.5], [30, 356.5], [40, 456.5]];
    await testAggregation('avg', expected);
  });


  test('sum', async () => {
    const expected = [[10, 1565], [20, 2565], [30, 3565], [40, 4565]];
    await testAggregation('sum', expected);
  });


  test('count', async () => {
    const expected = [[10, 10], [20, 10], [30, 10], [40, 10]];
    await testAggregation('count', expected);
  });

  test('rate', async () => {
    const expected = [[10, 1], [20, 1], [30, 1], [40, 1]];
    await testAggregation('rate', expected);
  });

  test('first', async () => {
    const expected = [[10, 131], [20, 231], [30, 331], [40, 431]];
    await testAggregation('first', expected);
  });


  test('last', async () => {
    const expected = [[10, 184], [20, 284], [30, 384], [40, 484]];
    await testAggregation('last', expected);
  });


  test('range', async () => {
    const expected = [[10, 74], [20, 74], [30, 74], [40, 74]];
    await testAggregation('range', expected);
  });

  test('stats', async () => {
    const raw_data = await insertAggregationData(TIMESERIES_KEY);
    const filtered = raw_data.filter(([id, val]) => id >= 10 && id <= 50);

    const buckets = {};
    const bucketIds = new Set();
    filtered.forEach(([id, val]) => {
      let round = (id - (id % 10));
      bucketIds.add(round);
      buckets[round] = buckets[round] || [];
      buckets[round].push(val);
    });
    bucketIds.forEach(id => {
      buckets[id] = calc_stats(buckets[id]);
    });

    const expected = Array.from(bucketIds).sort().map(ts => [ts, buckets[ts]]);

    const response = await getRange(client, TIMESERIES_KEY, 10, 50, 'AGGREGATION', 'stats', 10);
    // convert strings to floats in server response
    const actual = response.map(([ts, data]) => {
      const value = data.value;
      Object.keys(value).forEach(k => {
        value[k] = parseFloat(value[k]);
      });
      return [ts, value];
    });

    // for now, just make sure we have objects returned with the proper shape

    expect(actual).toEqual(expected);
  });

  test('distinct', async () => {
    const start_ts = 1488823384;
    const samples_count = 50;

    const data = [];

    const states = ['ready', 'active', 'waiting', 'complete'];
    const jobs = ['preparation', 'execution', 'cleanup'];

    for (let ts = start_ts, i = 0; i < samples_count; i++, ts++) {
      const job = jobs[i % jobs.length];
      const state = states[i % states.length];
      data.push({
        ts,
        id: i,
        job,
        state
      })
    }
    await insertData(client, TIMESERIES_KEY, start_ts, samples_count, data);

    const buckets = {};
    const bucketIds = new Set();
    data.forEach((rec) => {
      let round = (rec.ts - (rec.ts % 10));
      bucketIds.add(round);
      buckets[round] = buckets[round] || {};
      let slot = buckets[round];
      slot[rec.job] = 1
    });

    const expected = Array.from(bucketIds).sort().map(ts => [ts, {job: Object.keys(buckets[ts]).sort()} ]);

    const response = await getRange(client, TIMESERIES_KEY, '-', '+', 'LABELS', 'job', 'AGGREGATION', 'distinct', 10);

    // for now, just make sure we have objects returned with the proper shape
    response.forEach(x => {
      x[1].job.sort();
    });

    expect(response).toEqual(expected);
  });

  test('count_distinct', async () => {
    const start_ts = 1488823384;
    const samples_count = 50;

    const data = [];

    const states = ['ready', 'active', 'waiting', 'complete'];
    const jobs = ['preparation', 'execution', 'cleanup'];

    for (let ts = start_ts, i = 0; i < samples_count; i++, ts++) {
      const job = jobs[i % jobs.length];
      const state = states[i % states.length];
      data.push({
        ts,
        id: i,
        job,
        state
      })
    }
    await insertData(client, TIMESERIES_KEY, start_ts, samples_count, data);

    const buckets = {};
    const bucketIds = new Set();
    data.forEach((rec) => {
      let round = (rec.ts - (rec.ts % 10));
      bucketIds.add(round);
      buckets[round] = buckets[round] || {};
      let slot = buckets[round];
      slot[rec.state] = parseInt(slot[rec.state] || 0) + 1
    });

    const expected = Array.from(bucketIds).sort().map(ts => [ts, buckets[ts]]);

    const response = await getRange(client, TIMESERIES_KEY, '-', '+', 'LABELS', 'state', 'AGGREGATION', 'count_distinct', 10);
    // convert strings to floats in server response
    const actual = response.map(([ts, data]) => {
      return [ts, data.state];
    });

    // for now, just make sure we have objects returned with the proper shape

    expect(actual).toEqual(expected);
  });
});
