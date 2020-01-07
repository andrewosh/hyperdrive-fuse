const fs = require('fs')
const test = require('tape')
const hyperdrive = require('hyperdrive')
const ram = require('random-access-memory')
const rimraf = require('rimraf')
const xattr = require('fs-xattr')
const { exec } = require('child_process')
const Fuse = require('fuse-native')

const { HyperdriveFuse } = require('..')

test('can read/write a small file', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 100
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can read/write a large file', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 10000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can read/write a huge file', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 100000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can list a directory', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    await new Promise(resolve => {
      fs.mkdir('./mnt/a', err => {
        t.error(err, 'no error')
        fs.writeFile('./mnt/a/1', '1', err => {
          t.error(err, 'no error')
          fs.writeFile('./mnt/a/2', '2', err => {
            t.error(err, 'no error')
            fs.readdir('./mnt/a', (err, list) => {
              t.error(err, 'no error')
              t.same(list, ['1', '2'])
              return resolve()
            })
          })
        })
      })
    })
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can create and read from a symlink', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    await new Promise(resolve => {
      fs.writeFile('./mnt/a', 'hello', err => {
        t.error(err, 'no error')
        fs.symlink('a', './mnt/b', err => {
          t.error(err, 'no error')
          fs.readFile('./mnt/b', { encoding: 'utf-8' }, (err, content) => {
            t.error(err, 'no error')
            t.same(content, 'hello')
            return resolve()
          })
        })
      })
    })
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test.skip('a hanging get will be aborted after a timeout', async t => {
  const drive = hyperdrive(ram)
  const handlers = getHandlers(drive, './mnt')

  // Create an artificial hang
  handlers.chmod = (path, uid, gid, cb) => {}

  const { destroy } = await mount(drive, handlers, './mnt')
  process.once('SIGINT', () => cleanup(destroy))

  await new Promise(resolve => {
    fs.writeFile('./mnt/hello', 'goodbye', err => {
      t.error(err, 'no error')
      fs.chmod('./mnt/hello', 777, err => {
        t.true(err)
        t.same(err.errno, Fuse.EIO)
        return resolve()
      })
    })
  })

  await cleanup(destroy)
  t.end()
})

test.only('can write a file with xattrs', async t => {
  const drive = hyperdrive(ram)
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  await fs.promises.writeFile('test.txt', 'hello')
  await xattr.set('test.txt', 'key', 'value')
  await new Promise((resolve, reject) => {
    exec('cp test.txt ./mnt/hello.txt', err => {
      if (err) return reject(err)
      return resolve()
    })
  })
  const contents = await fs.promises.readFile('./mnt/hello.txt', { encoding: 'utf8' })
  t.same(contents, 'hello')

  // await fs.promises.unlink('test.txt')
  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

async function writeData (numSlices, sliceSize) {
  const content = Buffer.alloc(sliceSize * numSlices).fill('0123456789abcdefghijklmnopqrstuvwxyz')
  let slices = new Array(numSlices).fill(0).map((_, i) => content.slice(sliceSize * i, sliceSize * (i + 1)))
  let fd = await open('./mnt/hello', 'w+')
  for (let slice of slices) {
    await write(fd, slice, 0)
  }
  await close(fd)
  return content
}

async function readData (content, numSlices, sliceSize, readSize) {
  let fd = await open('./mnt/hello', 'r')
  let numReads = 0
  do {
    const pos = numReads * readSize
    const buf = Buffer.alloc(readSize)
    let bytesRead = await read(fd, buf, 0, readSize, pos)
    if (!buf.slice(0, bytesRead).equals(content.slice(pos, pos + readSize))) {
      throw new Error(`Slices do not match at position: ${pos}`)
    }
  } while (++numReads * readSize < numSlices * sliceSize)
  await close(fd)
}

async function cleanup (fuse, exit) {
  await fuse.unmount()
  return new Promise((resolve, reject) => {
    rimraf('./mnt', err => {
      if (err) return reject(err)
      if (exit) return process.exit(0)
      return resolve()
    })
  })
}

function read (fd, buf, offset, len, pos) {
  return new Promise((resolve, reject) => {
    fs.read(fd, buf, offset, len, pos, (err, bytesRead) => {
      if (err) return reject(err)
      return resolve(bytesRead)
    })
  })
}

function write (fd, buf, offset, len) {
  return new Promise((resolve, reject) => {
    fs.write(fd, buf, offset, len, (err, bytesWritten) => {
      if (err) return reject(err)
      return resolve(bytesWritten)
    })
  })
}

function open (f, flags) {
  return new Promise((resolve, reject) => {
    fs.open(f, flags, (err, fd) => {
      if (err) return reject(err)
      return resolve(fd)
    })
  })
}

function close (fd) {
  return new Promise((resolve, reject) => {
    fs.close(fd, err => {
      if (err) return reject(err)
      return resolve(err)
    })
  })
}
