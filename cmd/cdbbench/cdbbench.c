/* cdbbench.c */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <openssl/sha.h>

#include <rocksdb/c.h>
#include <kvs_api.h>


#define Static  static __attribute__((unused))


// strlcpy
Static char *slcpy(char *t, int tl, char *s, int sl)
{
    int i;
    for (i = 0; i < tl - 1 && i < sl && *s; i++, s++)
        t[i] = *s;
    t[i] = 0;
    return t;
}


/* pseudo random number generator.
 * copied from <http://burtleburtle.net/bob/rand/smallprng.html>
 */

typedef struct prand_t prand_t;
struct prand_t {
    uint64_t a, b, c, d;
};

Static uint64_t prand(prand_t *x)
{
#define Rot(_x_,_k_) (((_x_) << (_k_)) | ((_x_) >> (64-(_k_))))

    uint64_t e = x->a - Rot(x->b, 7);
    x->a = x->b ^ Rot(x->c, 13);
    x->b = x->c + Rot(x->d, 37);
    x->c = x->d + e;
    x->d = e + x->a;
    return x->d;

#undef Rot
}

Static void prand_seed(prand_t *x, uint64_t seed)
{
    uint64_t i;
    x->a = 0xf1ea5eed, x->b = x->c = x->d = seed;
    for (i = 0; i < 20; i++)
        prand(x);
}

// split by white spaces. 't' gets [char *, int] pairs.
Static int split(char **t, int tl, char *s)
{
    int ix;
    char *p;

    ix = 0;
    p = NULL;
    while (*s) {
        while (*s && strchr(" \t\r\n", *s) != NULL)
            s++;
        if (!*s)
            break;
        p = s;
        while (*s && strchr(" \t\r\n", *s) == NULL)
            s++;
        if (ix < tl / 2 - 1) {
            t[ix * 2] = p;
            t[ix * 2 + 1] = (char *) (s - p);
            ix++;
        }
    }
    return ix;
}

typedef struct db_t db_t;
struct db_t {
    int (*close)(void *);
    int (*put)(void *, char *, int, char *, int);
    int (*get)(void *, char *, int, char **, int *);
};

/* kvssd utility functions */

struct kvssd_t {
    int (*close)(void *);
    int (*put)(void *, char *, int, char *, int);
    int (*get)(void *, char *, int, char **, int *);

    kvs_device_handle dev;
    kvs_container_handle ch;
};

Static int kvssd_close(struct kvssd_t *kvs)
{
    kvs_close_container(kvs->ch);
    kvs_exit_env();
    return 0;
}

Static int kvssd_drop(struct kvssd_t *kvs, char *container_name)
{
    return kvs_delete_container(kvs->dev, container_name);
}

Static int kvssd_put(struct kvssd_t *kvs, char *key, int keylen, char *value, int valuelen)
{
    kvs_store_context put_ctx = { { KVS_STORE_POST, false }, NULL, NULL };
    kvs_key k = { key, (kvs_key_t) keylen };
    kvs_value v = { value, (uint32_t) valuelen, 0, 0 };
    return kvs_store_tuple(kvs->ch, &k, &v, &put_ctx);
}

Static int kvssd_get(struct kvssd_t *kvs, char *key, int keylen, char **value, int *valuelen)
{
    kvs_retrieve_context get_ctx = { { false, false }, NULL, NULL };
    kvs_key k = { key, (kvs_key_t) keylen };
    kvs_value v;
    int rc, sz = 1024, repeat = 2;

    while (repeat-- > 0) {
        char *buf = (char *) malloc(sz);
        if (buf == NULL)
            return ENOMEM;

        v.value = buf;
        v.length = sz;
        v.actual_value_size = 0;
        v.offset = 0;
        rc = kvs_retrieve_tuple(kvs->ch, &k, &v, &get_ctx);
        if (rc == 0) {
            if (sz > (int) v.actual_value_size) {
                buf[v.actual_value_size] = 0;   // null-terminate it
                *value = buf;
                *valuelen = v.actual_value_size;
                return 0;
            } else {
                free(buf);
                sz = (v.actual_value_size + 1 + 31) / 32 * 32;
                continue;
            }
        } else if (rc == KVS_ERR_VALUE_LENGTH_INVALID) {
            free(buf);
            sz = (v.actual_value_size + 1 + 31) / 32 * 32;
            continue;
        } else {
            free(buf);
            *value = NULL;
            *valuelen = 0;
            return rc;
        }
    }
    return KVS_ERR_VALUE_LENGTH_INVALID;
}

Static char *kvssd_info(struct kvssd_t *kvs, char *t, int tl)
{
    kvs_device dev_info;
    kvs_container container_info;
    int rc, tix = 0;

    if ((rc = kvs_get_device_info(kvs->dev, &dev_info)) != 0) {
        snprintf(t, tl, "%s", kvs_errstr(rc));
        return t;
    }
    if ((rc = kvs_get_container_info(kvs->ch, &container_info)) != 0) {
        snprintf(t, tl, "%s", kvs_errstr(rc));
        return t;
    }

    uint64_t unused = (uint64_t) dev_info.unalloc_capacity;

#define Out(_fmt_, ...) do {                                            \
    if (tix < tl)                                                       \
        tix += snprintf(t + tix, tl - tix, _fmt_, ##__VA_ARGS__);       \
} while (0)

    Out("capacity: %ld\n"
        "unalloc_capacity: %lu\n"
        "max_value_len: %u\n"
        "max_key_len: %u\n"
        "optimal_value_len: %u\n"
        "optimal_value_granularity: %u\n"
        "%s:\n"
        "  opened: %d\n"
        "  scale: %u\n"
        "  capacity: %lu\n"
        "  free_size: %lu\n"
        "  count: %lu\n",
        dev_info.capacity,
        unused,
        dev_info.max_value_len,
        dev_info.max_key_len,
        dev_info.optimal_value_len,
        dev_info.optimal_value_granularity,
        container_info.name && container_info.name->name ? container_info.name->name : "",
        container_info.opened,
        container_info.scale,
        container_info.capacity,
        container_info.free_size,
        container_info.count);
    return t;

#undef Out
}

Static int kvssd_open(struct kvssd_t *kvs, char *dev_name, char *container_name)
{
    kvs_init_options opts;
    kvs_container_context ctx;
    int rc;

    memset(kvs, 0, sizeof(struct kvssd_t));
    memset(&opts, 0, sizeof(opts));
    memset(&ctx, 0, sizeof(ctx));

    kvs_init_env_opts(&opts);
    opts.memory.use_dpdk = 0;
    opts.udd.core_mask_str[0] = '0';
    opts.udd.core_mask_str[1] = 0;
    opts.udd.cq_thread_mask[0] = '0';
    opts.udd.cq_thread_mask[1] = 0;
    opts.udd.mem_size_mb = 1024;
    opts.udd.syncio = 1;
    opts.emul_config_file = "dummy";
    kvs_init_env(&opts);

    if ((rc = kvs_open_device(dev_name, &kvs->dev)) != 0)
        return rc;

    ctx.option.ordering = KVS_KEY_ORDER_NONE;
    kvs_create_container(kvs->dev, container_name, 0, &ctx);
    kvs_open_container(kvs->dev, container_name, &kvs->ch);

    kvs->close = (int (*)(void *)) kvssd_close;
    kvs->put = (int (*)(void *, char *, int, char *, int)) kvssd_put;
    kvs->get = (int (*)(void *, char *, int, char **, int *)) kvssd_get;

    return 0;
}


// rocksdb functions

typedef struct rocks_t rocks_t;
struct rocks_t {
    int (*close)(void *);
    int (*put)(void *, char *, int, char *, int);
    int (*get)(void *, char *, int, char **, int *);

    rocksdb_t *db;
    rocksdb_options_t *opts;
    rocksdb_writeoptions_t *wopts;
    rocksdb_readoptions_t *ropts;
};

Static int rocks_close(rocks_t *rdb)
{
    rocksdb_options_destroy(rdb->opts);
    rocksdb_writeoptions_destroy(rdb->wopts);
    rocksdb_readoptions_destroy(rdb->ropts);
    rocksdb_close(rdb->db);
    memset(rdb, 0, sizeof(rocks_t));
    return 0;
}

Static int rocks_put(rocks_t *rdb, char *key, int keylen, char *value, int valuelen)
{
    char *err = NULL;
    rocksdb_put(rdb->db, rdb->wopts, key, keylen, value, valuelen, &err);
    return err == NULL ? 0 : -1;
}

Static int rocks_get(rocks_t *rdb, char *key, int keylen, char **value, int *valuelen)
{
    char *err = NULL;
    *value = rocksdb_get(rdb->db, rdb->ropts, key, (size_t) keylen, (size_t *) valuelen, &err);
    return err == 0 ? 0 : -1;
}

Static int rocks_open(rocks_t *rdb, char *file)
{
    char *err = NULL;

    rdb->opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(rdb->opts, 1);
    //rocksdb_options_set_max_open_files(rdb->opts, 1024);
    rdb->wopts = rocksdb_writeoptions_create();
    rdb->ropts = rocksdb_readoptions_create();

    rdb->db = rocksdb_open(rdb->opts, file, &err);
    if (err != NULL) {
        fprintf(stderr, "Cannot open db %s: %s\n", file, err);
        return -1;
    }

    rdb->close = (int (*)(void *)) rocks_close;
    rdb->put = (int (*)(void *, char *, int, char *, int)) rocks_put;
    rdb->get = (int (*)(void *, char *, int, char **, int *)) rocks_get;
    return 0;
}


// t should be big enough to fit.
Static char *pack4(char *t, char *s)
{
    int l = strlen(s), l2;
    if (l % 4 == 0)
        return s;
    l2 = (l + 3) / 4 * 4;
    memcpy(t, s, l);
    for ( ; l < l2; l++)
        t[l] = ' ';
    t[l2] = 0;
    return t;
}

Static int64_t mtime(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    // TODO: check if tv_nsec is indeed micro seconds, not nano.
    return (int64_t) ts.tv_sec * 1000 + ((int64_t) ts.tv_nsec / (1000 * 1000)) % 1000;
}

// out should be 32 bytes long
Static char *sha256(char *out, char *in, int inlen)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, in, inlen);
    SHA256_Final((unsigned char *) out, &ctx);
    return out;
}

// out should be 16 bytes long
Static char *sha1(char *out, char *in, int inlen)
{
    SHA_CTX ctx;
    SHA_Init(&ctx);
    SHA_Update(&ctx, in, inlen);
    SHA_Final((unsigned char *) out, &ctx);
    return out;
}

Static char *gen_val(char *t, int sz, char *key) {
    prand_t r;
    uint64_t v;
    int i, l;

    // ignore overrun
    prand_seed(&r, *((uint64_t *) t));

    l = sz / 8;
    for (i = 0; i < l; i++) {
        v = prand(&r);
        *((uint64_t *) t) = v;
        t += 8;
    }

    l = sz % 8;
    v = prand(&r);
    if (l >= 4) {
        *((uint32_t *) t) = (v >> 32) & 0xFFFFFFFF;
        v >>= 32;
        t += 4;
        l -= 4;
    }
    if (l >= 2) {
        *((uint16_t *) t) = (v >> 16) & 0xFFFF;
        v >>= 16;
        t += 2;
        l -= 2;
    }
    if (l >= 1) {
        *((uint8_t *) t) = (v >> 8) & 0xFF;
        v >>= 8;
        t += 1;
        l -= 1;
    }
    return t;
}

Static int get_min_max(db_t *db, char *prefix, uint64_t *min, uint64_t *max)
{
    char key[1024], *value, *ss[10];
    int rc, valuelen, ssl;

    // ignore overrun
    pack4(key, prefix);

    if ((rc = db->get(db, key, strlen(key), &value, &valuelen)) != 0)
        return rc;
    if (value == NULL)
        return -1;

    ssl = sizeof(ss) / sizeof(char *);
    ssl = split(ss, ssl, value);
    if (ssl != 2) {
        free(value);
        return -1;
    }

    slcpy(key, sizeof(key), ss[0], (int64_t) ss[1]);
    *min = strtoul(key, NULL, 10);
    slcpy(key, sizeof(key), ss[2], (int64_t) ss[3]);
    *max = strtoul(key, NULL, 10);

    free(value);
    return 0;
}

Static int set_min_max(db_t *db, char *prefix, uint64_t min, uint64_t max)
{
    char key[1024], t[1024];
    uint64_t old_min, old_max;

    if (get_min_max(db, prefix, &old_min, &old_max) == 0) {
        if (min > old_min)
            min = old_min;
        if (max < old_max)
            max = old_max;
    }
    pack4(key, prefix);
    snprintf(t, sizeof(t), "%lu %lu", min, max);
    return db->put(db, key, strlen(key), t, strlen(t));
}

struct worker_param_t {
    struct db_t *db;
    char *prefix;
    int64_t start, end, ix;
    uint64_t min, max;
    int value_size;
    int verbose;
};

Static void *writer(void *param)
{
    struct worker_param_t *p = (struct worker_param_t *) param;
    char key[32], _value[1024], *value = _value;
    int64_t ix;
    int rc, cnt = 0;

    if ((size_t) p->value_size > sizeof(_value))
        value = (char *) malloc(p->value_size);

    while (1) {
        ix = __sync_fetch_and_add(&p->ix, 1);
        if (ix > p->end)
            break;

        snprintf(_value, sizeof(_value), "%s-%ld", p->prefix, ix);
        sha256(key, _value, strlen(_value));
        gen_val(value, p->value_size, key);

        if ((rc = p->db->put(p->db, key, sizeof(key), value, p->value_size)) != 0) {
            printf("put failed for sha256('%s-%ld'): %s\n", p->prefix, ix,
                   kvs_errstr(rc));
            break;
        } else {
            cnt++;
        }
    }

    if (value && value != _value)
        free(value);
    return (void *) (int64_t) cnt;
}

Static int do_write(db_t *db, int n_threads, char *prefix, int start, int count, int value_size)
{
    struct worker_param_t wp;
    pthread_t tids[512];
    int rc;

    if ((size_t) n_threads > sizeof(tids) / sizeof(pthread_t)) {
        printf("Too many threads %d > %lu\n", n_threads, sizeof(tids) / sizeof(pthread_t));
        return -1;
    }

    wp.db = db;
    wp.prefix = prefix;
    wp.start = start;
    wp.end = start + count - 1;
    wp.ix = wp.start;
    wp.value_size = value_size;
    wp.verbose = 0;

    for (int i = 0; i < n_threads; i++) {
        if ((rc = pthread_create(tids + i, NULL, writer, &wp)) != 0) {
            printf("Cannot start a thread: %s\n", strerror(rc));
            return -1;
        }
    }

    // wait for them
    for (int i = 0; i < n_threads; i++) {
        int64_t cnt;
        // not checking return status or value
        pthread_join(tids[i], (void **) &cnt);
    }

    set_min_max(db, prefix, start, start + count - 1);
    return 0;
}

Static void *rreader(void *param)
{
    struct worker_param_t *p = (struct worker_param_t *) param;
    char key[32], t[1024], *value;
    prand_t r;
    uint64_t ix;
    int rc, valuelen;

    prand_seed(&r, (uint64_t) time(NULL));
    while (1) {
        ix = __sync_fetch_and_add(&p->ix, 1);
        if (ix > (uint64_t) p->end)
            break;

        ix = p->min + prand(&r) % (p->max - p->min + 1);
        snprintf(t, sizeof(t), "%s-%lu", p->prefix, ix);
        sha256(key, t, strlen(t));

        rc = p->db->get(p->db, key, sizeof(key), &value, &valuelen);
        if (rc != 0) {
            if (p->verbose)
                printf("failure: %s\n", kvs_errstr(rc));
        } else {
            free(value);
        }
    }
    return NULL;
}

// returns range size or -1 on error
Static int do_rread(db_t *db, int n_threads, char *prefix, int count, int verbose)
{
    struct worker_param_t wp;
    pthread_t tids[512];
    int rc;

    if ((size_t) n_threads > sizeof(tids) / sizeof(pthread_t)) {
        printf("Too many threads %d > %lu\n", n_threads, sizeof(tids) / sizeof(pthread_t));
        return -1;
    }

    wp.db = db;
    wp.prefix = prefix;
    wp.start = 1;
    wp.end = count;
    wp.ix = wp.start;
    wp.verbose = verbose;

    if (get_min_max(db, prefix, &wp.min, &wp.max) != 0)
        return -1;
    if (verbose)
        printf("min-max: %lu %lu\n", wp.min, wp.max);

    for (int i = 0; i < n_threads; i++) {
        if ((rc = pthread_create(tids + i, NULL, rreader, &wp)) != 0) {
            printf("Cannot start a thread: %s\n", strerror(rc));
            return -1;
        }
    }

    // wait for them
    for (int i = 0; i < n_threads; i++) {
        int64_t cnt;
        // not checking return status or value
        pthread_join(tids[i], (void **) &cnt);
    }

    return (int) (wp.max - wp.min + 1);
}

Static void usage(char *prog)
{
    char *p;
    if ((p = strrchr(prog, '/')))
        p++;
    else
        p = prog;
    printf(
"Usage: %s [<options>...] <db-name>\n\
    [write <prefix> <start> <count> <batch> <value-size>]\n\
    [rread <prefix> <count>] [info] [drop <container-name>]\n\
\n\
options:\n\
-H:     no header\n\
-t rocksdb|kvssd:       choose between rocksdb or kvssd (rocksdb).\n\
-d <device-name>:       where to collect disk stats from ("")\n\
-r <num-threads>:       number of read threads (1)\n\
-w <num-threads>:       number of write threads (1)\n\
-v:     verbose\n\
\n\
It's going to use about 1035 file descriptors, so don't forget to set open file descriptor limit to 2048, .e.g \"ulimit -n 2048\".\n\n", prog);
}

Static void header() {
    printf("@,OP,Prefix,Start/Range,Count,Time,Elap,TPS,DB(KB),R(#),R(KB),R(KB/s),W(#),W(KB),W(KB/s),DbR(#),DbR(KB),DbR(KB/s),DbW(#),DbW(KB),DbW(KB/s),Has(#),Del(#)\n");
}

Static void pre(int64_t *t)
{
    *t = mtime();
}

Static void post(char *head, int64_t ot, int64_t count)
{
    int64_t dur = mtime() - ot;
    if (dur <= 0)
        dur = 1;
    printf("%s,%ld,%ld,%.3f,%d,", head, ot / 1000, dur / 1000,
           (double) count * 1000.0 / (double) dur, 0);
    printf("0,0,0,0,0,0,0,0,0,0,0,0,0,0,0\n");
    fflush(stdout);
}

int main(int argc, char *argv[])
{
    struct rocks_t rdb;
    struct kvssd_t kvs;
    db_t *db;
    char **nargv, *dev_name = NULL, *db_type = NULL, *db_name = NULL;
    int nargc = 0, read_threads = 1, write_threads = 1, verbose = 0,
        no_header = 0;
    int c, rc;

    while ((c = getopt(argc, argv, "d:r:t:vw:hH")) != -1) {
        switch (c) {
        case 'd':
            dev_name = optarg;
            break;
        case 'r':
            read_threads = atoi(optarg);
            break;
        case 't':
            db_type = optarg;
            break;
        case 'v':
            verbose = 1;
            break;
        case 'w':
            write_threads = atoi(optarg);
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        case 'H':
            no_header = 1;
            break;
        }
    }

    nargc = argc - optind;
    nargv = argv + optind;

    if (nargc <= 0 || db_type == NULL ||
        !(strcmp(db_type, "kvssd") == 0 || strcmp(db_type, "rocksdb") == 0)) {
        usage(argv[0]);
        return 0;
    }

    db_name = *nargv++;
    nargc--;

    if (strcmp(db_type, "kvssd") == 0) {
        if ((rc = kvssd_open(&kvs, db_name, (char *) "meta")) != 0) {
            fprintf(stderr, "Failed to open kvssd %s: %s\n",
                    db_name, kvs_errstr(rc));
            return 1;
        }
        db = (db_t *) &kvs;
    } else if (strcmp(db_type, "rocksdb") == 0) {
        if ((rc = rocks_open(&rdb, db_name)) != 0) {
            fprintf(stderr, "Failed to open rocksdb %s\n", db_name);
            return 1;
        }
        db = (db_t *) &rdb;
    } else {
        fprintf(stderr, "Unknown db type %s\n", db_type);
        return 1;
    }

    if (nargc > 0) {
        if (nargc > 0 && strcmp(nargv[0], "info") == 0) {
            char info[1024];
            kvssd_info(&kvs, info, sizeof(info));
            printf("%s\n", info);
        } else if (nargc > 1 && strcmp(nargv[0], "drop") == 0) {
            int rc = kvssd_drop(&kvs, nargv[1]);
            if (rc == 0)
                printf("Dropped '%s' successfully.\n", nargv[1]);
            else
                printf("Failed to drop '%s': %s\n", nargv[1], kvs_errstr(rc));
        } else if (nargc >= 6 && strcmp(nargv[0], "write") == 0) {
            // write prefix start count batch size
            int start, count, value_size;
            int64_t ct;
            char head[512];

            if (!no_header)
                header();
            start = atoi(nargv[2]);
            count = atoi(nargv[3]);
            value_size = atoi(nargv[5]);
            pre(&ct);
            do_write(db, write_threads, nargv[1], start, count, value_size);
            snprintf(head, sizeof(head), "@,write,%s,%d,%d",
                     nargv[1], start, count);
            post(head, ct, count);
        } else if (nargc >= 3 && strcmp(nargv[0], "rread") == 0) {
            // rread prefix count
            int rc, count;
            int64_t ct;
            char head[512];

            if (!no_header)
                header();
            count = atoi(nargv[2]);
            pre(&ct);
            rc = do_rread(db, read_threads, nargv[1], count, verbose);
            snprintf(head, sizeof(head), "@,rread,%s,%d,%d",
                     nargv[1], rc, count);
            post(head, ct, count);
        }
    } else {
        // read commands from stdin
        char line[1024];

        if (!no_header)
            header();

        while (fgets(line, sizeof(line), stdin)) {
            char *ss[32], t[512];
            int ssl;

            ssl = split(ss, sizeof(ss) / sizeof(char *), line);
            if (ssl > 0)
                slcpy(t, sizeof(t), ss[0], (int64_t) ss[1]);
            if (ssl >= 6 && strcmp(t, "write") == 0) {
                int start, count, value_size;
                int64_t ct;
                char head[512];

                // prefix: 2, 3
                start = atoi(slcpy(t, sizeof(t), ss[4], (int64_t) ss[5]));
                count = atoi(slcpy(t, sizeof(t), ss[6], (int64_t) ss[7]));
                // batch size: 8, 9
                value_size = atoi(slcpy(t, sizeof(t), ss[10], (int64_t)ss[11]));
                pre(&ct);
                slcpy(t, sizeof(t), ss[2], (int64_t) ss[3]),
                do_write(db, write_threads, t, start, count, value_size);
                snprintf(head, sizeof(head), "@,write,%s,%d,%d",
                         t, start, count);
                post(head, ct, count);
            } else if (ssl >= 3 && strcmp(t, "rread") == 0) {
                int rc, count;
                int64_t ct;
                char head[512];

                // prefix: 2, 3
                count = atoi(slcpy(t, sizeof(t), ss[4], (int64_t) ss[5]));
                slcpy(t, sizeof(t), ss[2], (int64_t) ss[3]),
                pre(&ct);
                rc = do_rread(db, read_threads, t, count, verbose);
                snprintf(head, sizeof(head), "@,rread,%s,%d,%d", t, rc, count);
                post(head, ct, count);
            } else if (ssl >= 1 && strcmp(t, "quit") == 0) {
                break;
            }
        }
    }

    db->close(db);
    return 0;

    // to supress gdb
    if (0) {
        printf("%s %s %s %d %d %d %d\n", dev_name, db_type, db_name, read_threads, write_threads, verbose, no_header);
    }
}

/* EOF */
