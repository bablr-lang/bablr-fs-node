import { open, opendir } from 'node:fs/promises';
import { ReadableStream } from 'node:stream/web';
import { Buffer } from 'node:buffer';
import { StreamIterable } from '@bablr/stream-iterator';
import { getDefaultHighWaterMark } from 'node:stream';

let autoAllocateChunkSize = getDefaultHighWaterMark();

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

function* __readFile(path) {
  let stream = new ReadableStream(new StreamSource(path));

  const reader = stream.getReader({ mode: 'byob' });

  let result;
  do {
    result = yield reader.read(Buffer.alloc(autoAllocateChunkSize));
    if (result.value !== undefined) yield* result.value;
  } while (!result.done);
}

export const readFile = (path) => {
  return new StreamIterable(__readFile(path));
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
