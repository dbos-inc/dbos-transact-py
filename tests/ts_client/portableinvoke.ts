import {DBOSClient} from '@dbos-inc/dbos-sdk';

const sysurl = process.argv[2];

async function doTest() {
  console.log(`Using DB URL: ${sysurl}`);
  const client = await DBOSClient.create({ systemDatabaseUrl: sysurl });
  try {
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
    // TODO console.assert(JSON.stringify(await client.getEvent(wfhs.workflowID, 'defstat', 2)) === JSON.stringify({ status: 'Happy' }));
    try {
      console.assert(JSON.stringify(await client.getEvent(wfhs.workflowID, 'nstat', 2)) === JSON.stringify({ status: 'Happy' }));
    }
    catch (e) {
      ++deserializationErrs;
      if (!(e as Error).message.includes('deserialization type py_pickle is not available')) throw e;
    }
    console.assert(JSON.stringify(await client.getEvent(wfhs.workflowID, 'pstat', 2)) === JSON.stringify({ status: 'Happy' }));

    // TODO
    // const { value: ddread } = await client.readStream(wfhs.workflowID, 'defstream').next();
    // console.assert(JSON.stringify(ddread) === JSON.stringify({ stream: 'OhYeah' }));
    try {
      const { value: dnread } = await client.readStream(wfhs.workflowID, 'nstream').next();
      console.assert(JSON.stringify(dnread) === JSON.stringify({ stream: 'OYeah' }));
    }
    catch (e) {
      ++deserializationErrs;
      if (!(e as Error).message.includes('deserialization type py_pickle is not available')) throw e;
    }  
    const { value: dpread } = await client.readStream(wfhs.workflowID, 'pstream').next();
    console.assert(JSON.stringify(dpread) === JSON.stringify({ stream: 'OhYeah' }));

    const rvs = await wfhs.getResult();
    console.assert(rvs === 's-1-k:v@"m"');

    if (deserializationErrs !== 2) {
      throw new Error("Expected 2 deserialization errors");
    }
  } finally {
    await client.destroy();
  }
}

doTest().then(()=>{}).catch((e)=>{console.error(e); process.exit(1)});