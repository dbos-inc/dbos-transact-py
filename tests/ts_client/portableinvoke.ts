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

    /*
    expect(await client.getEvent(wfhs.workflowID, 'defstat', 2)).toStrictEqual({ status: 'Happy' });
    expect(await client.getEvent(wfhs.workflowID, 'nstat', 2)).toStrictEqual({ status: 'Happy' });
    expect(await client.getEvent(wfhs.workflowID, 'pstat', 2)).toStrictEqual({ status: 'Happy' });
    const { value: ddread } = await client.readStream(wfhs.workflowID, 'defstream').next();
    expect(ddread).toStrictEqual({ stream: 'OhYeah' });
    const { value: dnread } = await client.readStream(wfhs.workflowID, 'nstream').next();
    expect(dnread).toStrictEqual({ stream: 'OhYeah' });
    const { value: dpread } = await client.readStream(wfhs.workflowID, 'pstream').next();
    expect(dpread).toStrictEqual({ stream: 'OhYeah' });
    */
    const rvs = await wfhs.getResult();
    console.assert(rvs === 's-1-k:v@"m"');
  } finally {
    await client.destroy();
  }
}

doTest().then(()=>{}).catch(console.error);