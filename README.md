# @bablr/fs

This package offers fast, responsive, efficient access to the filesystem. Node's APIs are used under the hood, the created data streams are exposed as stream iterators.

## Usage

```js
import { readFile, readDir, decodeUTF8 } from '@bablr/fs';

let fileIterable = decodeUTF8(
  readFile(import.meta.url),
);
let directoryIterable = readDir('./');
```

## Why stream iterables?

[Stream iterables](https://github.com/bablr-lang/stream-iterator) are a way of using iterators as an abstraction over asynchronous data access in Javascript. They propagate both sync-ness and async-ness, moving data in an efficient way comparable to how a bucket brigade moves buckets of water. Stream iterables offer a trifecta of desirable features:

- Abstraction: they doesn’t require the provider to copy the data or expose its internal structure (benefit over web streams)
- Throughput: stream iterators have high throughput and efficiency when used on streams of many small items because only some steps must wait (benefit over flat async iterators)
- Responsiveness: returning the first data from a 20GB file doesn’t cause a huge delay, it takes just the same amount of time as opening a one-chunk file (benefit over fs.openFileSync)

## Language support

Stream iterables are not yet supported natively by JS, yet they are still possible to construct and consume using libraries. They are a primitive data type in JS: nothing about them can be made any simpler. Compare this to the officially supported "[web streams](https://developer.mozilla.org/en-US/docs/Web/API/Streams_API)", nothing about which could be made more complex.

We are thus looking for a champion for a `Symbol.streamIterator` proposal to TC39.
