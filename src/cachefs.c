#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 29

#ifdef HAVE_CONFIG_H
#include "config.h"
#else
#define _GNU_SOURCE
#define PACKAGE_VERSION "unknown"
#endif

#include <fuse.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/timerfd.h>
#include <poll.h>
#include <stdlib.h>
#if HAVE_MEMFD_CREATE
#include <sys/mman.h>
#endif
#include <inttypes.h>
#include <sys/sendfile.h>
#include <stddef.h>

#define max(a, b) ({ __typeof(a) _a = (a); __typeof(b) _b = (b); _a > _b ? _a : _b; })
#define min(a, b) ({ __typeof(a) _a = (a); __typeof(b) _b = (b); _a < _b ? _a : _b; })

struct cachefs_range
{
  off_t start;
  off_t end;
};

struct cachefs_range_pair
{
  struct cachefs_range ranges[2];
};

struct cachefs_change
{
  struct timespec written;
  struct cachefs_range range;
};

struct cachefs_context
{
  int fd_cache;
  int fd_data;
  struct stat st_cache;
  struct stat st_data;
  struct cachefs_block_info *block_info;
  const char *file_name;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int thread_exit:1;
  int fsync:1;
  int strict_sync:1;
  int dirty_expire;
  int cache_expire;
  int dirty_history_size;
  int size_changes;
  struct cachefs_change *changes;
};

#ifdef DEBUG
static ssize_t
_pread (int fd, void *buf, size_t size, off_t offset)
{
  ssize_t ret = pread (fd, buf, size, offset);
  int e = errno;
  fprintf (stderr, "pread(%d, %p, %zu, %jd) = %zd", fd, buf, size,
	   (intmax_t) offset, ret);
  if (ret == -1)
    fprintf (stderr, " (%s)\n", strerror (e));
  else
    fprintf (stderr, "\n");
  errno = e;
  return ret;
}

static ssize_t
_pwrite (int fd, const void *buf, size_t size, off_t offset)
{
  ssize_t ret = pwrite (fd, buf, size, offset);
  int e = errno;
  fprintf (stderr, "pwrite(%d, %p, %zu, %jd) = %zd", fd, buf, size,
	   (intmax_t) offset, ret);
  if (ret == -1)
    fprintf (stderr, " (%s)\n", strerror (e));
  else
    fprintf (stderr, "\n");
  errno = e;
  return ret;
}

static int
_fallocate (int fd, int flags, off_t offset, off_t size)
{
  int ret = fallocate (fd, flags, offset, size);
  int e = errno;
  fprintf (stderr, "fallocate(%d, %d, %jd, %jd) = %d", fd, flags,
	   (intmax_t) offset, (intmax_t) size, ret);
  if (ret == -1)
    fprintf (stderr, " (%s)\n", strerror (e));
  else
    fprintf (stderr, "\n");
  errno = e;
  return ret;
}

static off_t
_lseek (int fd, off_t offset, int whence)
{
  off_t ret = lseek (fd, offset, whence);
  int e = errno;
  fprintf (stderr, "lseek(%d, %jd, %d) = %jd", fd, (intmax_t) offset, whence,
	   (intmax_t) ret);
  if (ret == -1)
    fprintf (stderr, " (%s)\n", strerror (e));
  else
    fprintf (stderr, "\n");
  errno = e;
  return ret;
}

static ssize_t
_sendfile (int fd_out, int fd_in, off_t * off, size_t size)
{
  off_t o = *off;
  ssize_t ret = sendfile (fd_out, fd_in, off, size);
  int e = errno;
  fprintf (stderr, "sendfile(%d, %d, %jd/*<-%jd*/, %zu) = %zd", fd_out, fd_in,
	   (intmax_t) * off, (intmax_t) o, size, ret);
  if (ret == -1)
    fprintf (stderr, " (%s)\n", strerror (e));
  else
    fprintf (stderr, "\n");
  errno = e;
  return ret;
}

static int
_pthread_mutex_lock (pthread_mutex_t * mutex)
{
  pthread_t tid = pthread_self ();
  fprintf (stderr, "%lu: pthread_mutex_lock(%p) start\n", tid, mutex);
  int ret = pthread_mutex_lock (mutex);
  fprintf (stderr, "%lu: pthread_mutex_lock(%p) end with %d\n", tid, mutex,
	   ret);
  return ret;
}

static int
_pthread_mutex_unlock (pthread_mutex_t * mutex)
{
  pthread_t tid = pthread_self ();
  fprintf (stderr, "%lu: pthread_mutex_unlock(%p) start\n", tid, mutex);
  int ret = pthread_mutex_unlock (mutex);
  fprintf (stderr, "%lu: pthread_mutex_unlock(%p) end with %d\n", tid, mutex,
	   ret);
  return ret;
}

static int
_pthread_cond_wait (pthread_cond_t * cond, pthread_mutex_t * mutex)
{
  pthread_t tid = pthread_self ();
  fprintf (stderr, "%lu: pthread_cond_wait(%p, %p) start\n", tid, cond,
	   mutex);
  int ret = pthread_cond_wait (cond, mutex);
  fprintf (stderr, "%lu: pthread_cond_wait(%p, %p) end with %d\n", tid, cond,
	   mutex, ret);
  return ret;
}

static int _pthread_cond_signal (pthread_cond_t *) __attribute__((unused));
static int
_pthread_cond_signal (pthread_cond_t * cond)
{
  pthread_t tid = pthread_self ();
  fprintf (stderr, "%lu: pthread_cond_signal(%p) start\n", tid, cond);
  int ret = pthread_cond_signal (cond);
  fprintf (stderr, "%lu: pthread_cond_signal(%p) end with %d\n", tid, cond,
	   ret);
  return ret;
}

static int
_pthread_cond_broadcast (pthread_cond_t * cond)
{
  pthread_t tid = pthread_self ();
  fprintf (stderr, "%lu: pthread_cond_broadcast(%p) start\n", tid, cond);
  int ret = pthread_cond_broadcast (cond);
  fprintf (stderr, "%lu: pthread_cond_broadcast(%p) end with %d\n", tid, cond,
	   ret);
  return ret;
}

#define pread _pread
#define pwrite _pwrite
#define fallocate _fallocate
#define lseek _lseek
#define sendfile _sendfile
#define pthread_mutex_lock _pthread_mutex_lock
#define pthread_mutex_unlock _pthread_mutex_unlock
#define pthread_cond_wait _pthread_cond_wait
#define pthread_cond_signal _pthread_cond_signal
#define pthread_cond_broadcast _pthread_cond_broadcast

#endif

static int
cachefs_readdir (const char *path, void *buf, fuse_fill_dir_t filler,
		 off_t offset, struct fuse_file_info *fi)
{
  if (strcmp (path, "/") != 0)
    return -ENOENT;
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  filler (buf, ".", NULL, 0);
  filler (buf, "..", NULL, 0);
  filler (buf, cc->file_name, &cc->st_data, 0);
  return 0;
}

static int
cachefs_open (const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  if (*path == '/' && strcmp (path + 1, cc->file_name) != 0)
    return -ENOENT;
  fi->fh = fi->flags & O_ACCMODE;
  return 0;
}

static int
cachefs_read (const char *path, char *buf, size_t size, off_t offset,
	      struct fuse_file_info *fi)
{
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  size_t ret = 0;
  pthread_mutex_lock (&cc->mutex);
  while (ret < size)
    {
      off_t off_hole = lseek (cc->fd_cache, offset + ret, SEEK_HOLE);
      size_t size_data;
      if (off_hole == -1 && errno != ENXIO)
	{
	  pthread_mutex_unlock (&cc->mutex);
	  return -errno;
	}
      else if (off_hole == -1)
	{
	  size_data = size - ret;
	  off_hole = cc->st_cache.st_size;
	}
      else
	{
	  size_data = off_hole - (offset + ret);
	  if (size_data > size - ret)
	    size_data = size - ret;
	}
      if (size_data > 0)
	{
	  ssize_t r =
	    pread (cc->fd_cache, buf + ret, size_data, offset + ret);
	  if (r == -1 || r == 0)
	    {
	      pthread_mutex_unlock (&cc->mutex);
	      return r == -1 ? -errno : ret;
	    }
	  ret += r;
	}
      off_t off_data = lseek (cc->fd_cache, off_hole, SEEK_DATA);
      size_t size_hole;
      if (off_data == -1 && errno != ENXIO)
	{
	  pthread_mutex_unlock (&cc->mutex);
	  return -errno;
	}
      else if (off_data == -1)
	{
	  size_hole = size - ret;
	  off_data = cc->st_cache.st_size;
	}
      else
	{
	  size_hole = off_data - (offset + ret);
	  if (size_hole > size - ret)
	    size_hole = size - ret;
	}
      if (size_hole > 0)
	{
	  ssize_t r = pread (cc->fd_data, buf + ret, size_hole, offset + ret);
	  if (r == -1)
	    {
	      pthread_mutex_unlock (&cc->mutex);
	      return -errno;
	    }
	  if (r == 0)
	    {
	      pthread_mutex_unlock (&cc->mutex);
	      return ret;
	    }
	  ssize_t w = pwrite (cc->fd_cache, buf + ret, r, offset + ret);
	  if (w == -1)
	    {
	      pthread_mutex_unlock (&cc->mutex);
	      return -errno;
	    }
	  ret += w;
	}
    }
  pthread_mutex_unlock (&cc->mutex);
  return ret;
}

static int
cachefs_write (const char *path, const char *buf, size_t size, off_t offset,
	       struct fuse_file_info *fi)
{
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  size_t written = 0;
  size_t ret = 0;
  pthread_mutex_lock (&cc->mutex);
  while (cc->size_changes >= cc->dirty_history_size)
    pthread_cond_wait (&cc->cond, &cc->mutex);
  while (written < size)
    {
      ssize_t w = pwrite (cc->fd_cache, buf + ret, size - ret, offset + ret);
      if (w == -1)
	{
	  ret = -errno;
	  break;
	}
      written += w;
      ret = written;
    }
  int idx = cc->size_changes++;
  clock_gettime (CLOCK_MONOTONIC, &cc->changes[idx].written);
  cc->changes[idx].range.start = offset;
  cc->changes[idx].range.end = offset + size;
  pthread_cond_broadcast (&cc->cond);
  pthread_mutex_unlock (&cc->mutex);
  return ret;
}

static int
cachefs_getattr (const char *path, struct stat *st)
{
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  if (strcmp (path, "/") == 0)
    {
      if (fstat (cc->fd_data, st) == -1)
	return -errno;
      st->st_dev = 0;
      st->st_ino = 0;
      st->st_mode = 040777;
      st->st_nlink = 3;
      st->st_size = 0;
      st->st_blocks = 0;
      return 0;
    }
  else if (*path == '/' && strcmp (path + 1, cc->file_name) == 0)
    {
      if (fstat (cc->fd_data, st) == -1)
	return -errno;
      return 0;
    }
  return -ENOENT;
}

static int
cachefs_fsync (const char *path, int datasync, struct fuse_file_info *fi)
{
  struct fuse_context *fc = fuse_get_context ();
  struct cachefs_context *cc = fc->private_data;
  if (cc->strict_sync)
    {
      pthread_mutex_lock (&cc->mutex);
      cc->fsync = 1;
      pthread_cond_broadcast (&cc->cond);
      while (cc->fsync)
	pthread_cond_wait (&cc->cond, &cc->mutex);
      pthread_cond_broadcast (&cc->cond);
      pthread_mutex_unlock (&cc->mutex);
      return 0;
    }
  else
    {
      int ret;
      if (datasync)
	ret = fdatasync (cc->fd_cache);
      else
	ret = fsync (cc->fd_cache);
      if (ret == -1)
	{
	  pthread_mutex_unlock (&cc->mutex);
	  return -errno;
	}
      return 0;
    }
}

static void
cachefs_cache_expire_unchecked (struct cachefs_context *cc, off_t start,
				off_t end)
{
  if (start < end)
    fallocate (cc->fd_cache, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
	       start, end - start);
}

static void
cachefs_cache_expire_internal (struct cachefs_context *cc, int idx,
			       off_t start, off_t end)
{
  for (int i = idx; i < cc->size_changes; i++)
    {
      if (start >= end)
	return;
      off_t st = cc->changes[i].range.start;
      off_t en = cc->changes[i].range.end;
      if (en <= start || end <= st)
	continue;
      if (st <= start && end <= en)
	return;
      if (start < st && en < end)
	{
	  cachefs_cache_expire_internal (cc, i + 1, start, st);
	  start = en;
	  continue;
	}
      if (en < end)
	start = en;
      if (st > start)
	end = st;
    }
  cachefs_cache_expire_unchecked (cc, start, end);
}

static void
cachefs_cache_expire (struct cachefs_context *cc, off_t start, off_t end)
{
  cachefs_cache_expire_internal (cc, 0, start, end);
}

static ssize_t
cachefs_cache_update (struct cachefs_context *cc, off_t start, off_t end)
{
  start = lseek (cc->fd_data, start, SEEK_SET);
  if (start == -1)
    return -1;
  off_t offset = start;
  ssize_t s = sendfile (cc->fd_data, cc->fd_cache, &offset, end - start);
  if (s == -1)
    return -1;
  return s;
}

static void
cachefs_cache_expire_main (struct cachefs_context *cc)
{
  off_t off_data = 0;
  off_t off_hole;
  while (off_data != -1 && off_data < cc->st_cache.st_size)
    {
      pthread_mutex_lock (&cc->mutex);
      off_data = lseek (cc->fd_cache, off_data, SEEK_DATA);
      if (off_data == -1)
	goto no_data;
      off_hole = lseek (cc->fd_cache, off_data, SEEK_HOLE);
      if (off_hole == -1)
	off_hole = cc->st_cache.st_size;
      cachefs_cache_expire (cc, off_data, off_hole);
      off_data = off_hole;
      pthread_mutex_unlock (&cc->mutex);
    }
  return;
no_data:
  pthread_mutex_unlock (&cc->mutex);
}

static void *
cachefs_thread_cache_expire (void *data)
{
  struct cachefs_context *cc = data;
  struct timespec timeout;
  clock_gettime (CLOCK_MONOTONIC, &timeout);
  timeout.tv_sec += cc->cache_expire;
  while (1)
    {
      pthread_mutex_lock (&cc->mutex);
      int err = pthread_cond_timedwait (&cc->cond, &cc->mutex, &timeout);
      pthread_mutex_unlock (&cc->mutex);
      if (err == ETIMEDOUT)
	{
	  cachefs_cache_expire_main (cc);
	  timeout.tv_sec += cc->cache_expire;
	}
      if (cc->thread_exit)
	break;
    }
  return 0;
}

static void
cachefs_wait_written (struct cachefs_context *cc)
{
  while (cc->size_changes <= 0)
    {
      if (cc->thread_exit)
	return;
      if (cc->fsync)
	{
	  cc->fsync = 0;
	  fsync (cc->fd_data);
	  pthread_cond_broadcast (&cc->cond);
	}
      pthread_cond_wait (&cc->cond, &cc->mutex);
    }
}

static int
cachefs_waitfor_update (struct cachefs_context *cc)
{
  if (cc->thread_exit)
    return 1;
  if (cc->fsync)
    return 1;
  if (cc->size_changes >= cc->dirty_history_size)
    return 1;
  struct timespec timeout = cc->changes->written;
  timeout.tv_sec += cc->dirty_expire;
  int err = pthread_cond_timedwait (&cc->cond, &cc->mutex, &timeout);
  if (err == ETIMEDOUT)
    return 1;
  if (cc->thread_exit)
    return 1;
  if (cc->fsync)
    return 1;
  if (cc->size_changes >= cc->dirty_history_size)
    return 1;
  return 0;
}

static void
cachefs_update_changes (struct cachefs_context *cc, off_t start, off_t end)
{
  int idx_new = 0;
  int idx_old = 1;
  int size_changes = cc->size_changes;
  while (idx_old < size_changes)
    {
      off_t start_old = cc->changes[idx_old].range.start;
      off_t end_old = cc->changes[idx_old].range.end;
      if (start <= start_old && end_old <= end)
	{
	  idx_old++;
	  continue;
	}
      if (start <= start_old && start_old <= end)
	cc->changes[idx_old].range.start = end;
      else if (start <= end_old && end_old <= end)
	cc->changes[idx_old].range.end = start;
      cc->changes[idx_new++].range = cc->changes[idx_old++].range;
    }
  cc->size_changes = idx_new;
  pthread_cond_broadcast (&cc->cond);
}

static void
cachefs_update_next (struct cachefs_context *cc)
{
  off_t start = cc->changes->range.start;
  off_t end = cc->changes->range.end;
  ssize_t rsize = cachefs_cache_update (cc, start, end);
  if (rsize == -1)
    return;
  end = start + rsize;
  cachefs_update_changes (cc, start, end);
}

static void *
cachefs_thread_cache_write (void *data)
{
  struct cachefs_context *cc = data;
  pthread_mutex_lock (&cc->mutex);
  while (1)
    {
      cachefs_wait_written (cc);
      if (cc->thread_exit)
	break;
      if (cachefs_waitfor_update (cc))
	cachefs_update_next (cc);

    }
  pthread_mutex_unlock (&cc->mutex);
  return NULL;
}

static struct fuse_operations cachefs_operations = {
  .readdir = cachefs_readdir,
  .open = cachefs_open,
  .read = cachefs_read,
  .write = cachefs_write,
  .getattr = cachefs_getattr,
  .fsync = cachefs_fsync,
};

struct cachefs_options
{
  const char *data_file;
  const char *cache_file;
  const char *file_name;
  int dirty_expire;
  int cache_expire;
  int dirty_history_size;
  int strict_sync;
};

static void
cachefs_print_version (void)
{
  fprintf (stderr, "cachefs %s\n", PACKAGE_VERSION);
}

static void
cachefs_print_help (void *data, struct fuse_args *args)
{
  fuse_opt_add_arg (args, "-h");
  fuse_main (args->argc, args->argv, &cachefs_operations, data);
  fprintf (stderr, "\n");
  fprintf (stderr, "Cachefs options:\n");
  fprintf (stderr, "    -o data=DATAFILE        data file  (required)\n");
#if HAVE_MEMFD_CREATE
  fprintf (stderr,
	   "    -o cache=CACHEFILE      cache file (default: in memory)\n");
#else
  fprintf (stderr, "    -o cache=CACHEFILE      cache file (required)\n");
#endif
  fprintf (stderr,
	   "    -o name=FILE            file name (default: basename of DATAFILE)\n");
  fprintf (stderr,
	   "    -o dirty_expire=SEC     dirty cache expiration (default: 5)\n");
  fprintf (stderr, "    -o dirty_history_size=SIZE\n");
  fprintf (stderr,
	   "                            dirty history size (default: 1024)\n");
  fprintf (stderr,
	   "    -o cache_expire=SEC     cache expiration (default: 120)\n");
  fprintf (stderr,
	   "    -o [no]strict_sync      wait for sync on DATAFILE or not (default: nostrict_sync)\n");
}

static int
cachefs_opt_proc (void *data, const char *arg, int key,
		  struct fuse_args *outargs)
{
  switch (key)
    {
    case 1:
      cachefs_print_version ();
      exit (EXIT_SUCCESS);
    case 2:
      cachefs_print_help (data, outargs);
      exit (EXIT_SUCCESS);
    }
  return 1;
}

int
main (int argc, char *argv[])
{
  struct fuse_args args = FUSE_ARGS_INIT (argc, argv);
  struct cachefs_options opts = {
    .data_file = NULL,
    .cache_file = NULL,
    .file_name = NULL,
    .dirty_expire = 5,
    .cache_expire = 120,
    .dirty_history_size = 1024,
    .strict_sync = 0,
  };
  static const struct fuse_opt fuse_opts[] = {
    {"data=%s", offsetof (struct cachefs_options, data_file), 0},
    {"cache=%s", offsetof (struct cachefs_options, cache_file), 0},
    {"name=%s", offsetof (struct cachefs_options, file_name), 0},
    {"dirty_expire=%d", offsetof (struct cachefs_options, dirty_expire), 0},
    {"cache_expire=%d", offsetof (struct cachefs_options, cache_expire), 0},
    {"dirty_history_size=%d",
     offsetof (struct cachefs_options, dirty_history_size), 0},
    {"strict_sync", offsetof (struct cachefs_options, strict_sync), 1},
    {"nostrict_sync", offsetof (struct cachefs_options, strict_sync), 0},
    FUSE_OPT_KEY ("-V", 1),
    FUSE_OPT_KEY ("--version", 1),
    FUSE_OPT_KEY ("-h", 2),
    FUSE_OPT_KEY ("--help", 2),
    FUSE_OPT_END,
  };
  fuse_opt_parse (&args, &opts, fuse_opts, &cachefs_opt_proc);
  if (opts.data_file == NULL)
    {
      fprintf (stderr, "-o data=DATAFILE must be specified\n");
      cachefs_print_help (&opts, &args);
      exit (EXIT_FAILURE);
    }
  if (opts.file_name == NULL)
    {
      char *file_name = strrchr (opts.data_file, '/');
      if (file_name == NULL)
	opts.file_name = opts.data_file;
      else
	opts.file_name = file_name + 1;
    }
  int fd_data = open (opts.data_file, O_RDWR);
  if (fd_data == -1)
    {
      perror (opts.data_file);
      return 1;
    }
  struct stat st_data;
  if (fstat (fd_data, &st_data) == -1)
    {
      fprintf (stderr, "stat(\"%s\"): %m\n", opts.data_file);
      return 1;
    }
  int fd_cache;
  if (opts.cache_file != NULL)
    fd_cache = open (opts.cache_file, O_RDWR | O_CREAT, 0666);
  else
    {
#if HAVE_MEMFD_CREATE
      opts.cache_file = "<cachefd>";
      fd_cache = memfd_create (opts.cache_file, 0);
#else
      fprintf (stderr, "-o cache=CACHEFILE must be specified\n");
      cachefs_print_help (&opts, &args);
      exit (EXIT_FAILURE);
#endif
    }
  if (fd_cache == -1)
    {
      perror (opts.cache_file);
      return 1;
    }
  if (ftruncate (fd_cache, st_data.st_size) == -1)
    {
      perror ("ftruncate");
      return 1;
    }
  struct stat st_cache;
  if (fstat (fd_cache, &st_cache) == -1)
    {
      fprintf (stderr, "stat(\"%s\"): %m\n", opts.cache_file);
      return 1;
    }
  struct cachefs_context cc = {
    .fd_cache = fd_cache,
    .fd_data = fd_data,
    .st_cache = st_cache,
    .st_data = st_data,
    .file_name = opts.file_name,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .thread_exit = 0,
    .fsync = 0,
    .size_changes = 0,
    .dirty_expire = opts.dirty_expire,
    .cache_expire = opts.cache_expire,
    .dirty_history_size = opts.dirty_history_size,
    .strict_sync = opts.strict_sync,
  };
  pthread_condattr_t condattr;
  pthread_condattr_init (&condattr);
  pthread_condattr_setclock (&condattr, CLOCK_MONOTONIC);
  pthread_cond_init (&cc.cond, &condattr);
  cc.changes =
    malloc (sizeof (struct cachefs_change) * cc.dirty_history_size);
  if (cc.changes == NULL)
    {
      perror ("malloc");
      return 1;
    }
  char *mount_point;
  int multi_threaded;
  struct fuse *fuse = fuse_setup (args.argc, args.argv, &cachefs_operations,
				  sizeof (struct fuse_operations),
				  &mount_point,
				  &multi_threaded, &cc);
  if (fuse == NULL)
    return 1;
  pthread_t tid_write;
  pthread_create (&tid_write, NULL, cachefs_thread_cache_write, &cc);
  pthread_t tid_expire;
  pthread_create (&tid_expire, NULL, cachefs_thread_cache_expire, &cc);
  int ret = multi_threaded ? fuse_loop_mt (fuse) : fuse_loop (fuse);
  pthread_mutex_lock (&cc.mutex);
  cc.thread_exit = 1;
  pthread_cond_broadcast (&cc.cond);
  pthread_mutex_unlock (&cc.mutex);
  pthread_join (tid_write, NULL);
  pthread_join (tid_expire, NULL);
  fuse_teardown (fuse, mount_point);
  if (ret == -1)
    return 1;
  return 0;
}
