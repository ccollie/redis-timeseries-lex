const { createClient, parseObjectResponse } = require('./redis');

const TIMESERIES_KEY = 'ts:bulk-add';

describe('bulkAdd', () => {
  let client;

  beforeEach(async () => {
    client = await createClient();
    return client.flushdb();
  });

  afterEach(() => {
    return client.quit();
  });

  const names = ['alice', 'bob', 'charlie'];
  const states = ['ready', 'active', 'waiting', 'complete'];

  function random(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  function sample(arr) {
    const offset = random(0, arr.length-1);
    return arr[offset];
  }

  function generateRecord() {
    const data = {
      name: sample(names),
      state: sample(states),
      age: random(18, 65)
    };
    return data;
  }

  function generateData(count) {
    const start_ts = 1488823384;

    const data = new Array(count * 2);
    for(let i = 0; i < count; i++) {
      data.push(start_ts + i, generateRecord());
    }

    return data;
  }


  function callMethod(name, ...args) {
    return client.timeseries(TIMESERIES_KEY, name, ...args);
  }

  it('should add values to the set', async () => {
    const start_ts = 1488823384;
    const data = [];

    for(let i = 0; i < 20; i++) {
      data.push(start_ts + i, generateRecord());
    }

    const args = data.map((x, index) => {
      return (typeof x === 'object') ? JSON.stringify(x) : x
    })
    const added = await client.timeseries(TIMESERIES_KEY, 'bulkAdd', ...args);

    const size = await callMethod('size');
    expect(size).toEqual(20);

    const response = await callMethod('range', '-', '+', 'FORMAT', 'json').then(JSON.parse);
    const actual = response.reduce((res, x) => res.concat([x[0], x[1]]), []);
    expect(actual).toEqual(data);
  });

});
