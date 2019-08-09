const Redis = require('ioredis');
const pAll = require('p-all');
const path = require('path');
const fs = require('fs');

let script = null;
const scriptPath = path.resolve(__dirname, '../timeseries-lex.lua');


function load(redis) {
  return loadScriptFile().then(script => {
    redis.defineCommand('timeseries', {
      numberOfKeys: 1,
      lua: script
    })
  });
}

function loadScriptFile() {
  return new Promise((resolve, reject) => {
    if (script) return resolve(script);

    fs.readFile(scriptPath, { encoding: 'utf8' }, (err, data) => {
      if (err) return reject(err);
      resolve( script = data );
    });
  })
}

/**
 * Waits for a redis client to be ready.
 * @param {Redis} redis client
 */
function isRedisReady(client) {
  return new Promise((resolve, reject) => {
    if (client.status === 'ready') {
      resolve();
    } else {
      function handleReady() {
        client.removeListener('error', handleError);
        resolve();
      }

      function handleError(err) {
        client.removeListener('ready', handleReady);
        reject(err);
      }

      client.once('ready', handleReady);
      client.once('error', handleError);
    }
  });
}

function createClient() {
  const url = process.env.REDIS_URL || 'localhost:6379';
  const client = new Redis(url);   // todo - specify db ????
  // todo: wait for client ready event
  return isRedisReady(client).then(() => {
    return load(client).then(() => client);
  });
}

function execute(client, method, key, ...args) {
  return client.timeseries(key, method, ...args);
}

function insertData(client, key, start_ts, samples_count, data) {
  /*
  insert data to key, starting from start_ts, with 1 sec interval between them
  @param key: name of time_series
  @param start_ts: beginning of time series
  @param samples_count: number of samples
  @param data: could be a list of samples_count values, or one value. if a list, insert the values in their
  order, if not, insert the single value for all the timestamps
  */
  const calls = [];
  for (let i = 0; i < samples_count; i++) {
    let value = Array.isArray(data) ? data[i] : data;
    if (typeof(value) == 'object') {
      const args = Object.entries(value).reduce((res, [key, value]) => res.concat(key, (value == null) ? 'null' : value), []);
      calls.push( () => execute(client,'add', key, start_ts + i, ...args) )
    } else {
      calls.push( () => execute(client,'add', key, start_ts + i, 'value', value) )
    }
  }
  return pAll(calls, { concurrency: 8 });
}

async function addValues(client, key, ...args) {
  const values = [].concat(...args);
  const calls = [];
  for (let i = 0; i < values.length; i += 2) {
    const ts = values[i];
    const val = values[i+1];
    const call = () => client.timeseries(key, 'add', ts, 'value', val);
    calls.push(call);
  }

  return pAll(calls, { concurrency: 16 });
}

function parseObjectResponse(reply) {
  if (!Array.isArray(reply)) {
    return reply
  }
  const data = {};
  for (let i = 0; i < reply.length; i += 2) {
    let key = reply[i];
    let val = reply[i + 1];
    if (Array.isArray(val)) {
      data[key] = parseObjectResponse(val);
    } else {
      data[key] = val;
    }
  }
  return data
}

function parseAggregationResponse(reply) {
  if (!Array.isArray(reply)) {
    return [];
  }
  const data = [];
  for (let i = 0; i < reply.length; i += 2) {
    data.push(
        [ reply[i], parseObjectResponse(reply[i+1]) ]
    );
  }
  return data
}


function parseMessageResponse(reply) {
  if (!Array.isArray(reply)) {
    return [];
  }
  return reply.map(([id, val]) => {
    return [id, parseObjectResponse(val)]
  });
}

function getFormat(args) {
  let i = 0;
  for (i = 0; i < args.length; i++) {
    const arg = args[i];
    if (typeof arg === 'string' && (arg.toUpperCase() === 'FORMAT')) {
      if (i + 1 < args.length) {
        return args[i+1];
      }
    }
  }
  return null;
}

async function getRangeEx(client, cmd, key, min, max, ...args) {
  const isAggregation = args.find(x => typeof(x) === 'string' && x.toUpperCase() === 'AGGREGATION');
  const response = await client.timeseries(key, cmd, min, max, ...args);
  const format = getFormat(args);
  if (typeof response === 'string') {
    const format = getFormat(args);
    if (format === 'json')
      return JSON.parse(response);
  }
  if (isAggregation) {
    return parseAggregationResponse(response);
  }
  return parseMessageResponse(response);
}

async function getRange(client, key, min, max, ...args) {
  return getRangeEx(client, 'range', key, min, max, ...args);
}

async function getRevRange(client, key, min, max, ...args) {
  return getRangeEx(client, 'revrange', key, min, max, ...args);
}

async function copy(client, src, dest, min, max, ...args) {
  const sha = client.scriptsSet['timeseries'].sha;

  return client.evalsha(sha, 2, src, dest, 'copy', min, max, ...args);
}

module.exports = {
  createClient,
  insertData,
  addValues,
  getRange,
  getRevRange,
  parseObjectResponse,
  parseMessageResponse,
  copy
};