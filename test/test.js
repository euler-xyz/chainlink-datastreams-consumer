import { Consumer, Error, Report } from '../index.js';
import { config, feedIds } from './config.js';
import { createMockServer } from './mock.js';
import assert from 'node:assert';
import 'dotenv/config';
import { WebSocket as _WebSocket } from 'ws';
const WebSocket = _WebSocket || globalThis.WebSocket;

process.on('unhandledRejection', (reason, promise) => { throw reason });

const DEBUG = false;

let mockServer

before(async()=>{
  if (process.env.CHAINLINK_WS_MOCK_SERVER) {
    mockServer = createMockServer();
  } else {
    console.debug('Using real server from .env config.')
  }
});

after(async()=>{
  if (process.env.CHAINLINK_WS_MOCK_SERVER) {
    console.debug('Tearing down mock server.');
    mockServer.close();
  }
});

describe('entrypoint', function () {

  it('inits correctly', function () {
    assert.doesNotThrow(() => new Consumer({ ...config(), feedIds, }));
  });

  it('rejects deprecated config', function () {
    assert.throws(() => new Consumer({ clientID: 'x' }), Error.Config.DeprecatedConsumerId);
    assert.throws(() => new Consumer({ hostname: 'x' }), Error.Config.DeprecatedHostname);
    assert.throws(() => new Consumer({ wsHostname: 'x' }), Error.Config.DeprecatedWsHostname);
  });

});

describe('authentication', function () {

  it('allows clientSecret to be updated', function () {
    const client = new Consumer(config());
    assert.equal(client.clientSecret, config().clientSecret);
    const newSecret = 'something';
    client.clientSecret = newSecret;
    assert.equal(client.clientSecret, newSecret);
  });

  it('generates headers correctly', function () {
    const path = '/api/v1/ws';
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(',') }).toString();
    const headers = new Consumer(config()).generateHeaders('GET', path, search);
    // Check that the returned data is an object
    assert.strictEqual(typeof headers, 'object',
      'Returned data is not an object');
    // Check that all required keys are present and of the right types
    assert.ok(headers.Authorization,
      'Missing authorization');
    assert.strictEqual(typeof headers.Authorization, 'string',
      'Authorization is not a string');
    assert.ok(headers['X-Authorization-Timestamp'],
      'Missing timestamp');
    assert.strictEqual(typeof headers['X-Authorization-Timestamp'], 'string',
      'Timestamp is not a string');
    assert.ok(headers['X-Authorization-Signature-SHA256'],
      'Missing signature');
    assert.strictEqual(typeof headers['X-Authorization-Signature-SHA256'], 'string',
      'Signature is not a string');
  });

  it('does not generate headers without client id', function () {
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(','), })
    assert.throws(
      () => new Consumer({...config(), clientId: null})
        .generateHeaders('GET', '/api/v1/ws', search),
      err => err instanceof Error.Auth.NoClientId
    );
  });

  it('does not generate headers without client secret', function () {
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(','), })
    assert.throws(
      () => new Consumer({...config(), clientSecret: undefined})
        .generateHeaders('GET', '/api/v1/ws', search),
      err => err instanceof Error.Auth.NoClientSecret
    );
  });

  it('does not generate headers for wrong method', function () {
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(','), })
    assert.throws(
      () => new Consumer(config()).generateHeaders('RANDOM', '/api/v1/ws', search),
      err => err instanceof Error.Auth.InvalidHttpMethod
    );
  });

  it('does not generate headers for invalid path', function () {
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(','), })
    assert.throws(
      () => new Consumer(config()).generateHeaders('GET', 1230123, search),
      err => err instanceof Error.Auth.InvalidUrlPath,
    );
  });

  it('does not generate headers for wrong  search query params', function () {
    assert.throws(
      () => new Consumer(config()).generateHeaders('GET', '/api/v1/ws', 123),
      err => err instanceof Error.Auth.InvalidSearch,
    );
  });

  it('does not generate headers for invalid timestamp', function () {
    const search = new URLSearchParams({ feedIDs: [...feedIds].join(','), })
    assert.throws(
      () => new Consumer(config()).generateHeaders('GET', '/api/v1/ws', search, 'WRONGTIMESTAMP'),
      err => err instanceof Error.Auth.InvalidTimestamp,
    );
  });

})

describe('fetching', function () {

  it("can't fetch without apiUrl", function () {
    const client = new Consumer(config());
    delete client.apiUrl
    assert.rejects(()=>client.fetcher.fetch('/'))
  });

})

describe('subscribing', function () {

  it("can't subscribe to feeds without wsUrl", function () {
    assert.throws(() => new Consumer({...config(), wsUrl: null, feeds: []}));
    assert.throws(() => new Consumer({...config(), wsUrl: null, feeds: ['0x0']}));
  });

  it("doesn't allow connectedFeeds to be mutated directly", async function () {
    const client = new Consumer(config());
    assert.throws(() => client.feeds.add('0x0'));
    assert.throws(() => client.feeds.delete('0x0'));
    assert.throws(() => client.feeds.clear());
    await client.disconnect()
  });

  it("automatically disconnects when feeds are set to []", async function () {
    const client = new Consumer({...config(), feeds: feedIds, lazy: true });
    assert.strictEqual(client.feeds.size, feedIds.length);
    await client.unsubscribeAll()
    assert.strictEqual(client.feeds.size, 0);
    assert.strictEqual(client.socketState, WebSocket.CLOSED);
  });

  it('fetches a report for a single feed and validate the instance', async function () {
    for (const feed of feedIds) {
      const timestamp = Math.floor(Date.now() / 1000) - 30; // 30 seconds ago to avoid flaky test
      const report = await new Consumer(config()).fetchFeed({ timestamp, feed, });
      assert(report instanceof Report);
      if (DEBUG) {
        console.log({ feed, report });
      }
    }
  });

  it('fetches reports for multiple feeds and validate the type', async function () {
    const reports = await new Consumer(config()).fetchFeeds({
      timestamp: Math.floor(Date.now() / 1000), // current timestamp in seconds
      feeds: feedIds,
    });
    assert(typeof reports === 'object');
    if (DEBUG) {
      console.log({ feed, reports });
    }
  });

  it('receives reports when subscribed via constructor', function (done) {
    this.timeout(10000);
    const SDK = new Consumer({ ...config(), feeds: feedIds });
    SDK.once('report', async (report) => {
      await SDK.disconnect();
      done();
    });
  });

  it('receives reports when subscribed via method', async function () {
    this.timeout(10000);
    const SDK = new Consumer({ ...config() });
    await SDK.subscribeTo(feedIds[0])
    await new Promise((resolve, reject) =>
      SDK.once('report', ({ feedId }) => {
        if (feedId === feedIds[0]) {
          resolve()
        } else {
          reject(`Expected ${feedIds[0]}, got ${feedId}`)
        }
      }))
    await SDK.unsubscribeFrom(feedIds[0]);
    await SDK.subscribeTo(feedIds[1]);
    await new Promise((resolve, reject) =>
      SDK.once('report', ({ feedId }) => {
        if (feedId === feedIds[1]) {
          resolve()
        } else {
          reject(`Expected ${feedIds[1]}, got ${feedId}`)
        }
      }))
    await SDK.unsubscribeAll();
  });

  it('reconnects when socket closes', async function () {
    this.timeout(10000);
    const SDK = new Consumer({ ...config(), feeds: feedIds });
    await new Promise(resolve=>SDK.once('socket-message', async () => {
      console.log('Received 1st message, closing socket...')
      await SDK.socket.connection.close();
      resolve();
      console.log('Closed socket...');
    }));
    await new Promise(resolve=>SDK.once('socket-message', async () => {
      console.log('Reconnected, received 2nd message, closing for good...')
      await SDK.disconnect();
      resolve();
      console.log('Done!');
    }));
  });

})

describe('decoding', function () {

  it('throws an error when calling Report.fromSocketMessage with invalid data', function () {
    assert.throws(() => Report.fromSocketMessage({}), {
      name: 'Error',
    });
  });

  it('throws an error when calling Report.fromAPIResponse with invalid data', function () {
    assert.throws(() => Report.fromAPIResponse({}), {
      name: 'Error',
    });
  });

  it('throws an error when calling Report.fromBulkAPIResponse with invalid data', function () {
    assert.throws(() => Report.fromBulkAPIResponse({}), {
      name: 'Error',
    });
  });
});

describe('V8 schema', function () {

  it('should identify V8 feed ID correctly', function () {
    const v8FeedId = '0x0008000000000000000000000000000000000000000000000000000000000000';
    const version = Report.feedIdToVersion(v8FeedId);
    assert.strictEqual(version, 'v8');
  });

  it('should create V8 Report instance with correct properties', function () {
    const mockFullReport = {
      reportContext: ['0x0000000000000000000000000000000000000000000000000000000000000000'],
      reportBlob: {
        version: 'v8',
        decoded: {
          feedId: '0x0008000000000000000000000000000000000000000000000000000000000000',
          validFromTimestamp: 1000n,
          observationsTimestamp: 2000n,
          nativeFee: 100n,
          linkFee: 200n,
          expiresAt: 3000n,
          lastUpdateTimestamp: 2500n,
          midPrice: 50000n,
          marketStatus: 2n,
        }
      },
      rawRs: [],
      rawSs: [],
      rawVs: '0x0000000000000000000000000000000000000000000000000000000000000000',
      rawReport: '0x0000000000000000000000000000000000000000000000000000000000000000'
    };

    const report = new Report({
      feedID: '0x0008000000000000000000000000000000000000000000000000000000000000',
      validFromTimestamp: 1000,
      observationsTimestamp: 2000,
      fullReport: mockFullReport
    });

    assert.strictEqual(report.version, 'v8');
    assert.strictEqual(report.feedId, '0x0008000000000000000000000000000000000000000000000000000000000000');
    assert.strictEqual(report.validFromTimestamp, 1000n);
    assert.strictEqual(report.observationsTimestamp, 2000n);
    assert.strictEqual(report.nativeFee, 100n);
    assert.strictEqual(report.linkFee, 200n);
    assert.strictEqual(report.expiresAt, 3000n);
    assert.strictEqual(report.lastUpdateTimestamp, 2500n);
    assert.strictEqual(report.midPrice, 50000n);
    assert.strictEqual(report.marketStatus, 2n);
  });

  it('should handle V8 market status values correctly', function () {
    const marketStatuses = [0, 1, 2]; // Unknown, Closed, Open
    
    marketStatuses.forEach(status => {
      const mockFullReport = {
        reportContext: ['0x0000000000000000000000000000000000000000000000000000000000000000'],
        reportBlob: {
          version: 'v8',
          decoded: {
            feedId: '0x0008000000000000000000000000000000000000000000000000000000000000',
            validFromTimestamp: 1000n,
            observationsTimestamp: 2000n,
            nativeFee: 100n,
            linkFee: 200n,
            expiresAt: 3000n,
            lastUpdateTimestamp: 2500n,
            midPrice: 50000n,
            marketStatus: BigInt(status),
          }
        },
        rawRs: [],
        rawSs: [],
        rawVs: '0x0000000000000000000000000000000000000000000000000000000000000000',
        rawReport: '0x0000000000000000000000000000000000000000000000000000000000000000'
      };

      const report = new Report({
        feedID: '0x0008000000000000000000000000000000000000000000000000000000000000',
        validFromTimestamp: 1000,
        observationsTimestamp: 2000,
        fullReport: mockFullReport
      });

      assert.strictEqual(report.marketStatus, BigInt(status));
    });
  });

  it('should maintain backward compatibility with existing schemas', function () {
    const existingVersions = ['v1', 'v2', 'v3', 'v4'];
    
    existingVersions.forEach(version => {
      assert.ok(Report.reportBlobAbiSchema[version], 
        `Schema for ${version} should still be accessible`);
      assert.ok(Array.isArray(Report.reportBlobAbiSchema[version]), 
        `Schema for ${version} should be an array`);
    });

    assert.ok(Report.reportBlobAbiSchema.v8, 'V8 schema should be accessible');
    assert.ok(Array.isArray(Report.reportBlobAbiSchema.v8), 'V8 schema should be an array');
    
    const expectedV8Fields = 9;
    assert.strictEqual(Report.reportBlobAbiSchema.v8.length, expectedV8Fields,
      `V8 schema should have ${expectedV8Fields} fields`);
  });
});
