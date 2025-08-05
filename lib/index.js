import { open, opendir } from 'node:fs/promises';
import { ReadableStream } from 'node:stream/web';
import { Buffer } from 'node:buffer';
import { getStreamIterator, StreamIterable } from '@bablr/stream-iterator';
import { getDefaultHighWaterMark } from 'node:stream';

let autoAllocateChunkSize = getDefaultHighWaterMark();

let inRange = (v, min, max) => v > min && v < max;

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
  let step = iter.next();

  for (;;) {
    if (step instanceof Promise) {
      step = yield step;
    }

    if (step.done) break;

    let cp = step.value;

    if (cp <= 0xffff) {
      yield String.fromCharCode(cp);
    } else {
      cp -= 0x10000;
      yield String.fromCharCode((cp >> 10) + 0xd800, (cp & 0x3ff) + 0xdc00);
    }

    step = iter.next();
  }
}

export const decodeUTF8 = (bytes) => {
  return new StreamIterable(__decodeUTF8(bytes));
};

// adapted from https://github.com/inexorabletash/text-encoding/blob/master/lib/encoding.js
function* __codePointsFor(bytes) {
  let iter = getStreamIterator(bytes);
  let step = iter.next();
  let partialValue = null;

  for (;;) {
    if (step instanceof Promise) {
      step = yield step;
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

export const codePointsFor = (bytes) => {
  return new StreamIterable(__codePointsFor(bytes));
};

function* __readFile(path) {
  let stream = new ReadableStream(new StreamSource(path));

  const reader = stream.getReader({ mode: 'byob' });

  let result;
  do {
    result = yield reader.read(Buffer.alloc(autoAllocateChunkSize));
    if (result.value !== undefined) yield* result.value;
  } while (!result.done);
}

export const readFile = (path, options) => {
  let encoding = typeof options === 'string' ? options : options.encoding;

  if (encoding && !/utf-?8/i.test(encoding)) throw new Error('unsupported encoding');

  let bytes = new StreamIterable(__readFile(path, options));

  return encoding ? decodeUTF8(bytes) : bytes;
};

function* __readDir(path) {
  let dir = yield opendir(path);

  let break_ = false;
  let chunk = [];

  while (!break_) {
    for (let i = 0; i < 64; i++) {
      chunk.push(dir.read());
    }

    let values = yield Promise.all(chunk);

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

export const readDir = (path) => {
  return new StreamIterable(__readDir(path));
};
