import {DBOSClient} from '@dbos-inc/dbos-sdk';

const sysurl = process.argv[2];

function assertEqual(actual: unknown, expected: unknown, msg: string) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${msg}: expected ${e}, got ${a}`);
}

async function doTest() {
  console.log(`Using DB URL: ${sysurl}`);
  const client = await DBOSClient.create({ systemDatabaseUrl: sysurl });
  try {
    // --- Basic types test ---
    const wfhs = await client.enqueuePortable<string>(
      {
        workflowName: 'workflowPortable',
        workflowClassName: 'workflows',
        queueName: 'testq',
      },
      ['s', 1, { k: 'k', v: ['v'] }],
    );

    await client.send(wfhs.workflowID, 'm', 'incoming', undefined, { serializationType: 'portable' });

    let deserializationErrs = 0;
    assertEqual(await client.getEvent(wfhs.workflowID, 'defstat', 2), { status: 'Happy' }, 'defstat event');
    try {
      assertEqual(await client.getEvent(wfhs.workflowID, 'nstat', 2), { status: 'Happy' }, 'nstat event');
    }
    catch (e) {
      ++deserializationErrs;
      if (!(e as Error).message.includes('deserialization type py_pickle is not available')) throw e;
    }
    assertEqual(await client.getEvent(wfhs.workflowID, 'pstat', 2), { status: 'Happy' }, 'pstat event');

    const { value: ddread } = await client.readStream(wfhs.workflowID, 'defstream').next();
    assertEqual(ddread, { stream: 'OhYeah' }, 'defstream value');
    try {
      const { value: dnread } = await client.readStream(wfhs.workflowID, 'nstream').next();
      assertEqual(dnread, { stream: 'OhYeah' }, 'nstream value');
    }
    catch (e) {
      ++deserializationErrs;
      if (!(e as Error).message.includes('deserialization type py_pickle is not available')) throw e;
    }
    const { value: dpread } = await client.readStream(wfhs.workflowID, 'pstream').next();
    assertEqual(dpread, { stream: 'OhYeah' }, 'pstream value');

    assertEqual(await wfhs.getResult(), 's-1-k:v@"m"', 'basic workflow result');

    if (deserializationErrs !== 2) {
      throw new Error("Expected 2 deserialization errors");
    }
    console.log('Basic types test passed.');

    // --- Rich types test: dates, arrays, maps ---
    console.log('Testing rich types...');
    const richWfh = await client.enqueuePortable<Record<string, unknown>>(
      {
        workflowName: 'workflowRichTypes',
        workflowClassName: 'workflows',
        queueName: 'testq',
      },
      [
        new Date('2025-06-15T10:30:00Z'),
        [1, 'two', [3, 4], { nested: true }, null, 3.14],
        { a: 1, b: { c: [2, 3] }, d: 'hello', e: null },
      ],
    );

    // Send a complex message with a date, nested arrays and maps
    await client.send(
      richWfh.workflowID,
      { timestamp: new Date('2025-01-01T00:00:00Z'), data: [1, { x: 'y' }] },
      'rich_incoming', undefined, { serializationType: 'portable' },
    );

    // Validate events set by the Python workflow
    assertEqual(
      await client.getEvent(richWfh.workflowID, 'date_event', 10),
      '2025-06-15T10:30:00.000Z', 'date_event',
    );
    assertEqual(
      await client.getEvent(richWfh.workflowID, 'array_event', 10),
      [1, 'two', [3, 4], { nested: true }, null, 3.14], 'array_event',
    );
    assertEqual(
      await client.getEvent(richWfh.workflowID, 'map_event', 10),
      { a: 1, b: { c: [2, 3] }, d: 'hello', e: null }, 'map_event',
    );

    // Validate the workflow result
    const richResult = await richWfh.getResult();
    assertEqual(richResult.date_echo, '2025-06-15T10:30:00.000Z', 'date_echo');
    assertEqual(richResult.date_obj, '2025-06-15T10:30:00.000Z', 'date_obj');
    assertEqual(richResult.items, [1, 'two', [3, 4], { nested: true }, null, 3.14], 'items echo');
    assertEqual(richResult.items_count, 6, 'items_count');
    assertEqual(richResult.nested, { a: 1, b: { c: [2, 3] }, d: 'hello', e: null }, 'nested echo');
    assertEqual(richResult.nested_keys, ['a', 'b', 'd', 'e'], 'nested_keys');
    assertEqual(richResult.received_msg, {
      timestamp: '2025-01-01T00:00:00.000Z',
      data: [1, { x: 'y' }],
    }, 'received_msg');

    console.log('Rich types test passed!');
  } finally {
    await client.destroy();
  }
}

doTest().then(()=>{}).catch((e)=>{console.error(e); process.exit(1)});
