const { createClient, insertData, getRange, merge } = require('./redis');

const SOURCE_KEY = 'redis-ts-lex:merge:src1';
const SOURCE_KEY2 = 'redis-ts-lex:merge:src2';
const DEST_KEY = 'redis-ts-lex:merge:dest';

const first_names = ['Alice', 'Bob', 'Charlie', 'Denise', 'Erick', 'Fernanda', 'Gerald', 'Hermione', 'Idris', 'Jamilah'];
const last_names = ['Spring', 'Summer', 'Winters', 'May', 'March', 'Black', 'White', 'Green'];

const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const sample = (arr) => arr[rand(0, arr.length-1)];


const pick = (hash, opts = {}) => {

  let keys = Object.keys(hash);
  if (opts.redact) {
    keys = keys.filter(k => !opts.redact.includes(k));
  } else if (opts.labels) {
    keys = opts.labels;
  } else {
    return {...hash};
  }
  return keys.reduce((res, k) => {
    res[k] = hash[k];
    return res;
  }, {});
};

const DEFAULT_START_TS = 1511885909;

describe('merge', () => {
  let client;

  const start_ts = 1511885909;
  const samples_count = 50;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  function getHash(key = DEST_KEY) {
    return client.hgetall(key);
  }


  function mergeTs(first, second, opts) {

    const mergedSet = new Map();

    function updateSet(timeseries) {
      timeseries.forEach(([ts,value]) => {
        let hash = mergedSet.get(ts);
        if (!hash) {
          hash = {};
          mergedSet.set(ts, hash);
        }
        const val = pick(value, opts);
        const keys = Object.keys(val);
        keys.forEach(key => {
          const v = parseFloat(val[key]);
          if (!isNaN(v)) {
            hash[key] = (hash[key] || 0) + v;
          }
        });
      });
    }

    updateSet(first);
    updateSet(second);

    const timestamps = Array.from( mergedSet.keys() ).sort();
    const result = timestamps.reduce((res, ts) => {
      res.push([ts, mergedSet.get(ts)]);
      return res;
    }, []);
    return result;
  }


  async function testMerge(data1, data2, start_ts1 = DEFAULT_START_TS, start_ts2 = DEFAULT_START_TS, opts = {}) {
    await insertData(client, SOURCE_KEY , start_ts1, data1.length, data1);
    await insertData(client, SOURCE_KEY2 , start_ts2, data2.length, data2);

    const timeseries1 = await getRange(client, SOURCE_KEY, '-', '+');
    const timeseries2 = await getRange(client, SOURCE_KEY2, '-', '+');

    const expected = mergeTs(timeseries1, timeseries2, opts);

    let args = [];
    if (opts.labels) {
      args = ['LABELS', ...opts.labels];
    } else if (opts.redact) {
      args = ['REDACT', ...opts.redact];
    }

    if (Array.isArray(opts.options)) {
      args.push(...opts.options);
    }

    await merge(client, SOURCE_KEY, SOURCE_KEY2, DEST_KEY, '-', '+', ...args);

    let actual = await getRange(client, DEST_KEY, '-', '+');
    expect(actual).toEqual(expected);
  }

  it('should merge all values', async () => {
    const data = [];
    const data2 = [];

    for (let i = 0; i < samples_count; i++) {
      data.push( rand(1, 100) );
      data2.push( rand(1, 100) );
    }

    await insertData(client, SOURCE_KEY , start_ts, samples_count, data);
    await insertData(client, SOURCE_KEY2 , start_ts, samples_count, data2);

    const timeseries1 = await getRange(client, SOURCE_KEY, '-', '+');
    const timeseries2 = await getRange(client, SOURCE_KEY2, '-', '+');

    const expected = mergeTs(timeseries1, timeseries2);

    await merge(client, SOURCE_KEY, SOURCE_KEY2, DEST_KEY, '-', '+');

    const exists = await client.exists(DEST_KEY);

    expect(exists).toEqual(1);

    let actual = await getRange(client, DEST_KEY, '-', '+');
    expect(actual.length).toEqual(expected.length);
    expect(actual).toEqual(expected);

  });


  describe('options', () => {

    function generate(count = samples_count) {
      const data = [];
      for (let i=0; i < count; i++) {
        let obj = {
          age: rand(16, 65),
          cool_points: rand(10, 40000),
          last_name: sample(last_names),
          name: sample(first_names),
          income: rand(34500, 160000)
        }
        data.push(obj);
      }
      return data;
    }

    test('limit', async () => {
      const data = [];
      const data2 = [];

      for (let i = 0; i < samples_count; i++) {
        data.push(  rand(1, 100) );
        data2.push(  rand(1, 100) );
      }

      await insertData(client, SOURCE_KEY , start_ts, samples_count, data);
      await insertData(client, SOURCE_KEY2 , start_ts, samples_count, data2);

      await merge(client, SOURCE_KEY, SOURCE_KEY2, DEST_KEY, start_ts, start_ts + samples_count, 'LIMIT', 1, 4);
      const response = await getRange(client, DEST_KEY, start_ts, start_ts + samples_count);
      const actual = response.map(x => x[1].value);
      expect(actual.length).toEqual(4);
      expect(actual[0]).toEqual(data[1] + data2[1]);
      expect(actual[3]).toEqual(data[4] + data2[4]);
    });

    test('labels', async () => {

      const data = generate(10);
      const data2 = generate(10);

      const opts = {
        labels: ['cool_points', 'income']
      }

      await testMerge(data, data2, start_ts, start_ts, opts);
    });

    test('redact', async () => {

      const data = generate(10);
      const data2 = generate(10);

      const opts = {
        redact: ['name', 'last_name', 'id', 'age']
      }

      await testMerge(data, data2, start_ts, start_ts, opts);
    });

    describe('storage', () => {

      test('hash', async () => {

        const data = generate(10);
        const data2 = generate(10);

        await insertData(client, SOURCE_KEY, start_ts, data.length, data);
        await insertData(client, SOURCE_KEY2, start_ts, data2.length, data2);

        await merge(client, SOURCE_KEY, SOURCE_KEY2, DEST_KEY, start_ts, start_ts + data.length, 'STORAGE', 'hash' );

        const fields = ['age', 'cool_points', 'income']
        const response = await getHash(DEST_KEY);
        const actual = Object.keys(response).map(key => JSON.parse(response[key]))

        Object.keys(actual).forEach(k => {
          const obj = actual[k];
          expect(obj).toHaveProperty('age');
          expect(obj).toHaveProperty('cool_points');
          expect(obj).toHaveProperty('income');
        });

      });

    });

  });


});
