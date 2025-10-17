import Benchmark from 'benchmark';
//@ts-ignore
import { EventEmitter as CustomEmitter } from '../dist/';
import { EventEmitter as TseepEmitter } from 'tseep';
import { TypedEmitter } from 'tiny-typed-emitter';
import EventEmitter3 from 'eventemitter3';

console.log('='.repeat(80));
console.log('EVENT EMITTER BENCHMARK COMPARISON (FAIR)');
console.log('='.repeat(80));
console.log();

const suite = new Benchmark.Suite();

//@ts-ignore
const customEE1 = new CustomEmitter();
const tseepEE1 = new TseepEmitter();
const tinyEE1 = new TypedEmitter();
const ee3EE1 = new EventEmitter3();

customEE1.on('test', () => {});
tseepEE1.on('test', () => {});
tinyEE1.on('test', () => {});
ee3EE1.on('test', () => {});

//@ts-ignore
const customEE10 = new CustomEmitter();
const tseepEE10 = new TseepEmitter();
const tinyEE10 = new TypedEmitter();
const ee3EE10 = new EventEmitter3();

for (let i = 0; i < 10; i++) {
  customEE10.on('test', () => {});
  tseepEE10.on('test', () => {});
  tinyEE10.on('test', () => {});
  ee3EE10.on('test', () => {});
}

// Pre-setup instances for multiple args test
//@ts-ignore
const customEEArgs = new CustomEmitter();
const tseepEEArgs = new TseepEmitter();
const tinyEEArgs = new TypedEmitter();
const ee3EEArgs = new EventEmitter3();

customEEArgs.on('multi', () => {});
tseepEEArgs.on('multi', () => {});
tinyEEArgs.on('multi', () => {});
ee3EEArgs.on('multi', () => {});

suite
  // Single listener emit benchmark
  .add('CustomEmitter: emit (1 listener)', () => {
    customEE1.emit('test', 'data');
  })
  .add('tseep: emit (1 listener)', () => {
    tseepEE1.emit('test', 'data');
  })
  .add('tiny-typed-emitter: emit (1 listener)', () => {
    tinyEE1.emit('test', 'data');
  })
  .add('eventemitter3: emit (1 listener)', () => {
    ee3EE1.emit('test', 'data');
  })

  // 10 listeners emit benchmark
  .add('CustomEmitter: emit (10 listeners)', () => {
    customEE10.emit('test', 'data');
  })
  .add('tseep: emit (10 listeners)', () => {
    tseepEE10.emit('test', 'data');
  })
  .add('tiny-typed-emitter: emit (10 listeners)', () => {
    tinyEE10.emit('test', 'data');
  })
  .add('eventemitter3: emit (10 listeners)', () => {
    ee3EE10.emit('test', 'data');
  })

  // once + emit benchmark (this one needs setup per iteration)
  .add('CustomEmitter: once + emit', () => {
    //@ts-ignore
    const ee = new CustomEmitter();
    ee.once('test', () => {});
    ee.emit('test', 'data');
  })
  .add('tseep: once + emit', () => {
    const ee = new TseepEmitter();
    ee.once('test', () => {});
    ee.emit('test', 'data');
  })
  .add('tiny-typed-emitter: once + emit', () => {
    const ee = new TypedEmitter();
    ee.once('test', () => {});
    ee.emit('test', 'data');
  })
  .add('eventemitter3: once + emit', () => {
    const ee = new EventEmitter3();
    ee.once('test', () => {});
    ee.emit('test', 'data');
  })

  // Multiple args benchmark
  .add('CustomEmitter: emit (multiple args)', () => {
    customEEArgs.emit('multi', 42, 'test', true, { key: 'value' });
  })
  .add('tseep: emit (multiple args)', () => {
    tseepEEArgs.emit('multi', 42, 'test', true, { key: 'value' });
  })
  .add('tiny-typed-emitter: emit (multiple args)', () => {
    tinyEEArgs.emit('multi', 42, 'test', true, { key: 'value' });
  })
  .add('eventemitter3: emit (multiple args)', () => {
    ee3EEArgs.emit('multi', 42, 'test', true, { key: 'value' });
  })

  // on + off benchmark
  .add('CustomEmitter: on + off', () => {
    //@ts-ignore
    const ee = new CustomEmitter();
    const fn = () => {};
    ee.on('test', fn);
    ee.off('test', fn);
  })
  .add('tseep: on + off', () => {
    const ee = new TseepEmitter();
    const fn = () => {};
    ee.on('test', fn);
    ee.off('test', fn);
  })
  .add('tiny-typed-emitter: on + off', () => {
    const ee = new TypedEmitter();
    const fn = () => {};
    ee.on('test', fn);
    ee.off('test', fn);
  })
  .add('eventemitter3: on + off', () => {
    const ee = new EventEmitter3();
    const fn = () => {};
    ee.on('test', fn);
    ee.off('test', fn);
  })

  // Listener addition benchmark
  .add('CustomEmitter: add listener', () => {
    //@ts-ignore
    const ee = new CustomEmitter();
    ee.on('test', () => {});
  })
  .add('tseep: add listener', () => {
    const ee = new TseepEmitter();
    ee.on('test', () => {});
  })
  .add('tiny-typed-emitter: add listener', () => {
    const ee = new TypedEmitter();
    ee.on('test', () => {});
  })
  .add('eventemitter3: add listener', () => {
    const ee = new EventEmitter3();
    ee.on('test', () => {});
  })

  // emit with no args benchmark
  .add('CustomEmitter: emit (no args)', () => {
    customEE1.emit('test');
  })
  .add('tseep: emit (no args)', () => {
    tseepEE1.emit('test');
  })
  .add('tiny-typed-emitter: emit (no args)', () => {
    tinyEE1.emit('test');
  })
  .add('eventemitter3: emit (no args)', () => {
    ee3EE1.emit('test');
  })

  .on('cycle', (event: Benchmark.Event) => {
    console.log(String(event.target));
  })
  .on('complete', function (this: Benchmark.Suite) {
    console.log();
    console.log('='.repeat(80));
    console.log('RESULTS SUMMARY');
    console.log('='.repeat(80));
    console.log();

    const results: { [key: string]: Benchmark.Target[] } = {};

    this.forEach((bench: Benchmark.Target) => {
      const name = bench.name as string;
      const category = name.split(':')[1].trim();
      if (!results[category]) results[category] = [];
      results[category].push(bench);
    });

    Object.keys(results).forEach((category) => {
      const benches = results[category];
      const fastest = benches.reduce((prev, current) =>
        (current.hz || 0) > (prev.hz || 0) ? current : prev
      );

      console.log(`\n${category}:`);
      
      const tableData = benches.map((bench) => {
        const lib = (bench.name as string).split(':')[0];
        const ops = (bench.hz || 0).toFixed(0);
        const relative = ((bench.hz || 0) / (fastest.hz || 1) * 100).toFixed(2);
        const isFastest = bench === fastest;

        return {
          Library: lib,
          'ops/sec': parseInt(ops).toLocaleString(),
          'Relative': `${relative}%`,
          '': isFastest ? '⚡ FASTEST' : ''
        };
      });

      console.table(tableData);
    });

    console.log('='.repeat(80));
  })
  .run({ async: false });
