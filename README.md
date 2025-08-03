# @bablr/fs

This package offers fast, responsive, efficient access to the filesystem. Node's APIs are used under the hood, the created data streams are exposed as stream iterators. The stream exposes directories in chunks of 64.

## Usage

```js
import { readFile, readDir } from '@bablr/fs';

let fileIterable = readFile(import.meta.url);
let directoryIterable = readDir('./');
```

## Why stream iterables?

Stream iterables combine the throughput of synchronous iteration with the responsiveness of asynchronous iteration. They often take the form of iterators that _usually_ return results synchronously, but sometimes require you to wait if a result is not immediately available. This allows results to become available chunk-by-chunk without directly exposing any buffers.
