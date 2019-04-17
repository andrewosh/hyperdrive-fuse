const os = require('os')

const datEncoding = require('dat-encoding')
const mkdirp = require('mkdirp')
const fuse = require('fuse-native')
const fsConstants = require('filesystem-constants')
const { translate, linux } = fsConstants

const debug = require('debug')('hyperdrive-fuse')

function getHandlers (drive) {
  const handlers = {}

  handlers.getattr = function (path, cb) {
    debug('getattr', path)
    drive.stat(path, (err, stat) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      if (path === '/') {
        stat.uid = process.getuid()
        stat.gid = process.getgid()
      }
      return cb(0, stat)
    })
  }

  handlers.readdir = function (path, cb) {
    debug('readdir', path)
    drive.readdir(path, (err, files) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0, files)
    })
  }

  handlers.open = function (path, flags, cb) {
    debug('open', path, flags)

    const platform = os.platform()
    if (platform !== 'linux') {
      flags = translate(fsConstants[platform], linux, flags)
    }

    drive.open(path, flags, (err, fd) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0, fd)
    })
  }

  handlers.release = function (path, handle, cb) {
    debug('release', path, handle)
    drive.close(handle, err => {
      if (err) return cb(-err.errno || fuse.EBADF)
      return cb(0)
    })
  }

  handlers.releasedir = function (path, handle, cb) {
    // TODO: What to do here?
    return cb(0)
  }

  handlers.read = function (path, handle, buf, len, offset, cb) {
    debug('read', path, handle, len, offset)
    drive.read(handle, buf, 0, len, offset, (err, bytesRead) => {
      if (err) return cb(-err.errno || fuse.EBADF)
      return cb(bytesRead)
    })
  }

  handlers.write = function (path, handle, buf, len, offset, cb) {
    debug('write', path, handle, len, offset)

    // TODO: Duplicating the input buffer is a temporary patch for a race condition.
    // (Fuse overwrites the input before the data is flushed to storage in hypercore.)
    buf = Buffer.from(buf)

    drive.write(handle, buf, 0, len, (err, bytesWritten) => {
      if (err) return cb(-err.errno || fuse.EBADF)
      return cb(bytesWritten)
    })
  }

  handlers.truncate = function (path, size, cb) {
    debug('truncate', path, size)
    drive.truncate(path, size, err => {
      if (err) return cb(-err.errno || fuse.EPERM)
      return cb(0)
    })
  }

  handlers.unlink = function (path, cb) {
    debug('unlink', path)
    drive.unlink(path, err => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0)
    })
  }

  handlers.mkdir = function (path, mode, cb) {
    debug('mkdir', path)
    drive.mkdir(path, mode, err => {
      if (err) return cb(-err.errno || fuse.EPERM)
      return cb(0)
    })
  }

  handlers.rmdir = function (path, cb) {
    debug('rmdir', path)
    drive.rmdir(path, err => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0)
    })
  }

  handlers.create = function (path, mode, cb) {
    debug('create', path, mode)
    drive.create(path, { mode, uid: process.getuid(), gid: process.getgid() }, err => {
      if (err) return cb(err)
      drive.open(path, 'w', (err, fd) => {
        if (err) return cb(-err.errno || fuse.ENOENT)
        return cb(0, fd)
      })
    })
  }

  handlers.chown = function (path, uid, gid, cb) {
    debug('chown', path, uid, gid)
    drive._update(path, { uid, gid }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.chmod = function (path, mode, cb) {
    debug('chmod', path, mode)
    drive._update(path, { mode }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.utimens = function (path, actime, modtime, cb) {
    debug('utimens', path, actime, modtime)
    drive._update(path, {
      atime: actime.getTime(),
      mtime: modtime.getTime()
    }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.getxattr = function (path, name, buffer, length, offset, cb) {
    debug('getxattr')
    cb(0)
  }

  handlers.setxattr = function (path, name, buffer, length, offset, flags, cb) {
    debug('setxattr')
    cb(0)
  }

  handlers.statfs = function (path, cb) {
    cb(0, {
      bsize: 1000000,
      frsize: 1000000,
      blocks: 1000000,
      bfree: 1000000,
      bavail: 1000000,
      files: 1000000,
      ffree: 1000000,
      favail: 1000000,
      fsid: 1000000,
      flag: 1000000,
      namemax: 1000000
    })
  }

  return handlers
}

async function mount (drive, mnt, cb) {
  const prom = ready()
  if (cb) {
    prom.catch(err => cb(err))
    prom.then(obj => cb(null, obj))
  }

  return prom

  async function ready () {
    const handlers = getHandlers(drive)

    // handlers.options = ['allow_other']
    handlers.options = []
    if (debug.enabled) {
      handlers.options.push('debug')
    }

    return new Promise((resolve, reject) => {
      mkdirp(mnt, err => {
        if (err) return reject(err)
        drive.ready(err => {
          if (err) return reject(err)
          fuse.mount(mnt, handlers, err => {
            if (err) return reject(err)
            const keyString = datEncoding.encode(drive.key)
            return resolve({ mnt, handlers, key: keyString, drive })
          })
        })
      })
    })
  }
}

function unmount (mnt, cb) {
  const prom = new Promise((resolve, reject) => {
    fuse.unmount(mnt, err => {
      if (err) return reject(err)
      return resolve(err)
    })
  })
  if (cb) {
    prom.then(() => cb(null))
    prom.catch(err => cb(err))
  }
  return prom
}

module.exports = {
  mount,
  unmount,
  configure: fuse.configure,
  unconfigure: fuse.unconfigure
}
