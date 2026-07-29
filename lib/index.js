import { open, opendir } from 'node:fs/promises';
import { ReadableStream } from 'node:stream/web';
import { Buffer } from 'node:buffer';
import { continue_, getStreamIterator, StreamIterable, wait } from '@bablr/stream-iterator';
import { getDefaultHighWaterMark } from 'node:stream';
import { maybeWait, arrayFromStream } from '@bablr/stream-helpers';

let autoAllocateChunkSize = getDefaultHighWaterMark();

let inRange = (v, min, max) => v > min && v < max;

let compareEntityNames = (a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0);

class StreamSource {
  constructor(path) {
    this.path = path;
    this.type = 'bytes';
    this.autoAllocateChunkSize = autoAllocateChunkSize;
    this.controller = null;
  }

  async start(controller) {
    this.file = await open(this.path);
    this.controller = controller;
  }

  async pull(controller) {
    const view = controller.byobRequest?.view;
    const { bytesRead } = await this.file.read({
      buffer: view,
      offset: view.byteOffset,
      length: view.byteLength,
    });

    if (bytesRead === 0) {
      await this.file.close();
      this.controller.close();
    }
    controller.byobRequest.respond(bytesRead);
  }
}

function* __decodeUTF8(bytes) {
  let codePoints = codePointsFor(bytes);
  let iter = getStreamIterator(codePoints);
  let step;

  for (;;) {
    step = iter.next();
    while (step === null || step instanceof Promise) {
      if (step === null) yield continue_(), (step = iter.next());
      if (step instanceof Promise) step = yield wait(step);
    }
    if (step.done) break;

    let cp = step.value;

    if (cp <= 0xffff) {
      yield String.fromCharCode(cp);
    } else {
      cp -= 0x10000;
      yield String.fromCharCode((cp >> 10) + 0xd800, (cp & 0x3ff) + 0xdc00);
    }
  }
}

export const decodeUTF8 = (bytes) => {
  return new StreamIterable(__decodeUTF8(bytes));
};

export const codePointsFor = (bytes) => {
  return new StreamIterable(__codePointsFor(bytes));
};

// adapted from https://github.com/inexorabletash/text-encoding/blob/master/lib/encoding.js
function* __codePointsFor(bytes) {
  let iter = getStreamIterator(bytes);
  let step = iter.next();
  let partialValue = null;

  for (;;) {
    while (step === null || step instanceof Promise) {
      if (step === null) yield continue_(), (step = iter.next());
      if (step instanceof Promise) step = yield wait(step);
    }
    if (step.done) break;

    let { value } = step;

    if (partialValue) {
      if (inRange(value, 0xdc00, 0xdfff)) {
        yield (0x10000 + (partialValue & (0x3ff << 10)) + value) & 0x3ff;
      } else {
        yield 0xfffd;
      }

      partialValue = null;
    } else if (!inRange(value, 0xd800, 0xdfff)) {
      yield value;
    } else if (inRange(value, 0xdc00, 0xdfff)) {
      yield 0xfffd;
    } else if (inRange(value, 0xd800, 0xdbff)) {
      partialValue = value;
    }

    step = iter.next();
  }

  if (partialValue) {
    yield 0xfffd;
  }
}

export const readFile = (path) => {
  return new StreamIterable(__readFile(path));
};

function* __readFile(path) {
  let stream = new ReadableStream(new StreamSource(path));

  const reader = stream.getReader({ mode: 'byob' });

  let result;
  do {
    result = yield wait(reader.read(Buffer.alloc(autoAllocateChunkSize)));
    if (result.value !== undefined) yield* result.value;
  } while (!result.done);
}

function* __sortedDir(array, options) {
  let array_ = array;
  if (array_ instanceof Promise) {
    array_ = yield wait(array_);
  }

  array_.sort(options.porcelain ? compareEntityNames : options.sort);

  yield* array_;
}

export const readDir = (path, options = {}) => {
  let options_ = { ...options };
  if (options_.porcelain && options_.sort) throw new Error();

  let iter = new StreamIterable(__readDir(path, options_));

  if (options.sort || options.porcelain) {
    return new StreamIterable(getStreamIterator(__sortedDir(arrayFromStream(iter), options_)));
  } else {
    return iter;
  }
};

function* __readDir(path) {
  let dir = yield wait(opendir(path));

  let break_ = false;
  let chunk = [];

  while (!break_) {
    for (let i = 0; i < 64; i++) {
      chunk.push(dir.read());
    }

    let values = yield wait(Promise.all(chunk));

    for (let value of values) {
      if (value === null) {
        break_ = true;
      } else {
        yield value;
      }
    }

    chunk.length = 0;
  }
}
