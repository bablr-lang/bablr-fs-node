# @bablr/fs

This package offers fast, responsive, efficient access to the filesystem. Node's APIs are used under the hood, the created data streams are exposed as stream iterators.

## Usage

```js
import { readFile, readDir } from '@bablr/fs';

let fileIterable = readFile(import.meta.url, 'utf8');
let directoryIterable = readDir('./');
```

## Why stream iterables?

Stream iterables offer a trifecta of desirable features:

- Abstraction: they doesn’t require the provider to copy the data or expose its internal structure (benefit over web streams)
- Throughput: stream iterators have high throughput and efficiency when used on streams of many small items because only some steps must wait (benefit over flat async iterators)
- Responsiveness: returning the first data from a 20GB file doesn’t cause a huge delay, it takes just the same amount of time as opening a one-chunk file (benefit over fs.openFileSync style)
