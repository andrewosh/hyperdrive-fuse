const p = require('path')
const fs = require('fs')
const os = require('os')

const datEncoding = require('dat-encoding')
const mkdirp = require('mkdirp')
const fuse = require('fuse-native')
const fsConstants = require('filesystem-constants')
const { translate, linux } = fsConstants

const debug = require('debug')('hyperdrive-fuse')

function getHandlers (drive, mnt, opts = {}) {
  var handlers = {}
  const log = opts.log || debug

  handlers.getattr = function (path, cb) {
    log('getattr', path)
    drive.lstat(path, (err, stat) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      if (path === '/') {
        stat.uid = process.getuid()
        stat.gid = process.getgid()
      }
      return cb(0, stat)
    })
  }

  handlers.readdir = function (path, cb) {
    log('readdir', path)
    drive.readdir(path, (err, files) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0, files)
    })
  }

  handlers.open = function (path, flags, cb) {
    log('open', path, flags)

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
    log('release', path, handle)
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
    log('read', path, handle, len, offset)
    drive.read(handle, buf, 0, len, offset, (err, bytesRead) => {
      if (err) return cb(-err.errno || fuse.EBADF)
      return cb(bytesRead)
    })
  }

  handlers.write = function (path, handle, buf, len, offset, cb) {
    log('write', path, handle, len, offset)

    // TODO: Duplicating the input buffer is a temporary patch for a race condition.
    // (Fuse overwrites the input before the data is flushed to storage in hypercore.)
    buf = Buffer.from(buf)

    drive.write(handle, buf, 0, len, (err, bytesWritten) => {
      if (err) return cb(-err.errno || fuse.EBADF)
      return cb(bytesWritten)
    })
  }

  handlers.truncate = function (path, size, cb) {
    log('truncate', path, size)
    drive.truncate(path, size, err => {
      if (err) return cb(-err.errno || fuse.EPERM)
      return cb(0)
    })
  }

  handlers.unlink = function (path, cb) {
    log('unlink', path)
    drive.unlink(path, err => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0)
    })
  }

  handlers.mkdir = function (path, mode, cb) {
    log('mkdir', path)
    drive.mkdir(path, { mode, uid: process.getuid(), gid: process.getgid() }, err => {
      if (err) return cb(-err.errno || fuse.EPERM)
      return cb(0)
    })
  }

  handlers.rmdir = function (path, cb) {
    log('rmdir', path)
    drive.rmdir(path, err => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      return cb(0)
    })
  }

  handlers.create = function (path, mode, cb) {
    log('create', path, mode)
    drive.create(path, { mode, uid: process.getuid(), gid: process.getgid() }, err => {
      if (err) return cb(err)
      drive.open(path, 'w', (err, fd) => {
        if (err) return cb(-err.errno || fuse.ENOENT)
        return cb(0, fd)
      })
    })
  }

  handlers.chown = function (path, uid, gid, cb) {
    log('chown', path, uid, gid)
    drive._update(path, { uid, gid }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.chmod = function (path, mode, cb) {
    log('chmod', path, mode)
    drive._update(path, { mode }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.utimens = function (path, actime, modtime, cb) {
    log('utimens', path, actime, modtime)
    drive._update(path, {
      atime: actime.getTime(),
      mtime: modtime.getTime()
    }, err => {
      if (err) return cb(fuse.EPERM)
      return cb(0)
    })
  }

  handlers.getxattr = function (path, name, buffer, length, offset, cb) {
    log('getxattr')
    cb(0)
  }

  handlers.setxattr = function (path, name, buffer, length, offset, flags, cb) {
    log('setxattr')
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

  handlers.symlink = function (target, linkname, cb) {
    log('symlink', target, linkname)
    drive.symlink(target, linkname, err => {
      if (err) return cb(-err.errno || fuse.EPERM)
      return cb(0)
    })
  }

  handlers.readlink = function (path, cb) {
    log('readlink', path)
    drive.lstat(path, (err, st) => {
      if (err) return cb(-err.errno || fuse.ENOENT)
      const resolved = p.join(mnt, p.resolve('/', p.dirname(path), st.linkname))
      return cb(0, resolved)
    })
  }

  return handlers
}

async function mount (drive, handlers, mnt, opts) {
  if (typeof handlers === 'string') return mount(drive, null, handlers, opts)
  opts = opts || {}

  return ready()

  function ready () {
    handlers = handlers ? { ...handlers } : getHandlers(drive, mnt, opts)

    handlers.force = !!opts.force
    handlers.displayFolder = !!opts.displayFolder
    handlers.options = []
    if (debug.enabled || opts.debug) {
      handlers.options.push('debug')
    }

    return new Promise((resolve, reject) => {
      fs.stat(mnt, (err, stat) => {
        if (err && err.errno !== -2) return reject(err)
        if (err) {
          return mkdirp(mnt, err => {
            if (err) return reject(err)
            return mount()
          })
        }
        return mount()
      })

      function mount () {
        drive.ready(err => {
          if (err) return reject(err)
          fuse.mount(mnt, handlers, err => {
            if (err) return reject(err)
            const keyString = datEncoding.encode(drive.key)
            return resolve({ mnt, handlers, key: keyString, drive })
          })
        })
      }
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
  getHandlers,
  configure: fuse.configure,
  unconfigure: fuse.unconfigure,
  isConfigured: fuse.isConfigured,
}
