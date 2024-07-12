#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <threads.h>
#include <dirent.h>
#include <unistd.h>
#include <getopt.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <linux/limits.h>

#define WD_VERSION "0.1.2"

struct wd_queue_item {
    char path[PATH_MAX];
};

struct wd_queue {
    struct wd_queue_item *items;
    size_t len;
    size_t cap;
    atomic_flag flag;
    mtx_t mtx;
};

static int run_thread(void *param);
static void warm_dir(const char *path, struct wd_queue *q, size_t *count);
static void queue_flush_to(struct wd_queue *from, struct wd_queue *to);
static void queue_alloc(struct wd_queue *q, size_t cap);
static void queue_free(struct wd_queue *q);
static void queue_atomic_init(struct wd_queue *q);
static void queue_atomic_lock(struct wd_queue *q);
static void queue_atomic_unlock(struct wd_queue *q);
static void queue_atomic_deinit(struct wd_queue *q);
static void queue_mutex_init(struct wd_queue *q);
static void queue_mutex_lock(struct wd_queue *q);
static void queue_mutex_unlock(struct wd_queue *q);
static void queue_mutex_deinit(struct wd_queue *q);
static void parse_opts(int argc, char **argv);
static void usage(FILE *f, int exit_code);

enum wd_lock_type { ATOMIC, MUTEX };

static int opt_path_idx = 0;
static enum wd_lock_type opt_lock_type = ATOMIC;
static size_t opt_init_queue_size = 1024;
static int opt_num_threads = 8;
static int opt_print = 0;

static void (*queue_init)(struct wd_queue *q);
static void (*queue_lock)(struct wd_queue *q);
static void (*queue_unlock)(struct wd_queue *q);
static void (*queue_deinit)(struct wd_queue *q);


static struct wd_queue queue = {0};
static int num_active = 0;

int main(int argc, char **argv) {
    parse_opts(argc, argv);

    // Set function pointers for lock type
    if (opt_lock_type == ATOMIC) {
        queue_init = queue_atomic_init;
        queue_lock = queue_atomic_lock;
        queue_unlock = queue_atomic_unlock;
        queue_deinit = queue_atomic_deinit;
    } else {
        queue_init = queue_mutex_init;
        queue_lock = queue_mutex_lock;
        queue_unlock = queue_mutex_unlock;
        queue_deinit = queue_mutex_deinit;
    }

    // Alloc queue, threads, counts
    queue_alloc(&queue, opt_init_queue_size);
    thrd_t *threads = calloc(opt_num_threads, sizeof(thrd_t));
    size_t *counts = calloc(opt_num_threads, sizeof(size_t));

    // Init num_active
    num_active = opt_num_threads;

    // Seed the queue
    size_t total = 1;
    queue_lock(&queue);
    while (opt_path_idx < argc) {
        warm_dir(argv[opt_path_idx++], &queue, &total);
    }
    queue_unlock(&queue);

    // Run threads
    int i;
    for (i = 0; i < opt_num_threads; i++) {
        thrd_create(&threads[i], (thrd_start_t)run_thread, &counts[i]);
    }

    // Wait for threads
    for (i = 0; i < opt_num_threads; i++) {
        thrd_join(threads[i], NULL);
        total += counts[i];
    }

    // Free
    free(threads);
    free(counts);
    queue_free(&queue);

    // Print total
    fprintf(stderr, "%zu\n", total);

    return 0;
}

static int run_thread(void *param) {
    struct wd_queue_item *item;
    struct wd_queue local = {0};
    int starved = 0;
    char local_path[PATH_MAX];
    size_t *count = (size_t *)param;

    // Alloc local queue
    queue_alloc(&local, opt_init_queue_size);

    while (1) {
        queue_lock(&queue);

        // Flush local queue to main queue
        if (local.len > 0) queue_flush_to(&local, &queue);

        if (!starved) num_active -= 1; // Not active

        if (queue.len <= 0) {
            if (num_active <= 0) {
                // Empty queue, no one else warming == done
                queue_unlock(&queue);
                break;
            } else {
                // Empty queue, others are warming == starved
                if (!starved) starved = 1;
                queue_unlock(&queue);
                continue;
            }
        }

        // If we get here, we can dequeue
        starved = 0;
        num_active += 1;
        item = &queue.items[queue.len - 1]; // More of a stack I guess
        queue.len -= 1;

        // item->path will be unsafe after unlocking, so copy to local_path
        strcpy(local_path, item->path);

        queue_unlock(&queue);

        // Warm local_path
        warm_dir(local_path, &local, count);
    }

    // Free local queue
    queue_free(&local);

    return 0;
}

static void warm_dir(const char *path, struct wd_queue *q, size_t *count) {
    DIR *dir;
    struct dirent *ent;
    struct wd_queue_item *item;

    if (!(dir = opendir(path))) return;

    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_type == DT_DIR) {
            if (strcmp(ent->d_name, ".") == 0) continue;
            if (strcmp(ent->d_name, "..") == 0) continue;

            // Add directories to local queue
            queue_alloc(q, q->len + 1);
            item = &q->items[q->len];
            if (snprintf(item->path, PATH_MAX, "%s/%s", path, ent->d_name) < PATH_MAX) {
                q->len += 1;
                if (opt_print) printf("%s\n", item->path);
                *count += 1;
            }
        } else {
            struct stat st;
            char tmp[PATH_MAX];

            // Stat non-directories
            if (snprintf(tmp, PATH_MAX, "%s/%s", path, ent->d_name) < PATH_MAX) {
                stat(tmp, &st);
                if (opt_print) printf("%s\n", tmp);
                *count += 1;
            }
        }
    }

    closedir(dir);
}

static void queue_flush_to(struct wd_queue *from, struct wd_queue *to) {
    size_t new_len = from->len + to->len;

    queue_alloc(to, new_len);

    // Could avoid copying by adding some complexity, e.g., having threads
    // reserve slots on the main queue instead of using their own local queues.
    memcpy(
        to->items + to->len,
        from->items,
        from->len * sizeof(struct wd_queue_item)
    );

    to->len = new_len;
    from->len = 0;
}

static void queue_alloc(struct wd_queue *q, size_t cap) {
    if (q->cap >= cap) {
        // Already enough capacity
        return;
    }

    if (q->cap <= 0) q->cap = 1; // Shouldn't happen

    // Double capacity until sufficient
    do {
        q->cap *= 2;
    } while (q->cap < cap);

    // Reallocate
    q->items = realloc(q->items, q->cap * sizeof(struct wd_queue_item));
}

static void queue_free(struct wd_queue *q) {
    free(q->items);
}

static void queue_atomic_init(struct wd_queue *q) {
    atomic_flag_clear(&q->flag); // ATOMIC_FLAG_INIT
}

static void queue_atomic_lock(struct wd_queue *q) {
    while (atomic_flag_test_and_set(&q->flag));
}

static void queue_atomic_unlock(struct wd_queue *q) {
    atomic_flag_clear(&q->flag);
}

static void queue_atomic_deinit(struct wd_queue *q) {
    (void)q;
}

static void queue_mutex_init(struct wd_queue *q) {
    mtx_init(&q->mtx, mtx_plain);
}

static void queue_mutex_lock(struct wd_queue *q) {
    mtx_lock(&q->mtx);
}

static void queue_mutex_unlock(struct wd_queue *q) {
    mtx_unlock(&q->mtx);
}

static void queue_mutex_deinit(struct wd_queue *q) {
    mtx_destroy(&q->mtx);
}

static void parse_opts(int argc, char **argv) {
    int long_opt_i = 0;
    static struct option long_opts[] = {
        { "num-threads",     required_argument, 0, 'j' },
        { "init-queue-size", required_argument, 0, 's' },
        { "lock-free",       no_argument,       0, 'a' },
        { "lock-mutex",      no_argument,       0, 'm' },
        { "print",           no_argument,       0, 'p' },
        { "help",            required_argument, 0, 'h' },
        { "version",         required_argument, 0, 'V' },
        {0,                  0,                 0,  0  }
    };

    int c;
    while ((c = getopt_long(argc, argv, "j:s:amphV", long_opts, &long_opt_i)) != -1) {
        switch (c) {
            case 'j': opt_num_threads = atoi(optarg); break;
            case 's': opt_init_queue_size = (size_t)strtoull(optarg, NULL, 10); break;
            case 'a': opt_lock_type = ATOMIC; break;
            case 'm': opt_lock_type = MUTEX; break;
            case 'p': opt_print = 1; break;
            case 'h': usage(stdout, 0); break;
            case 'V': printf("warmdents v%s\n", WD_VERSION); exit(0); break;
            default: usage(stderr, 1); break;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "Expected path(s)\n\n");
        usage(stderr, 1);
    } else if (opt_num_threads <= 0) {
        fprintf(stderr, "Expected --num-threads >= 1\n\n");
        usage(stderr, 1);
    } else if (opt_init_queue_size <= 0) {
        fprintf(stderr, "Expected --init-queue-size >= 1\n\n");
        usage(stderr, 1);
    }

    opt_path_idx = optind;
}

static void usage(FILE *f, int exit_code) {
    fprintf(f, "Usage: warmdents [OPTION]... <PATH>...\n");
    fprintf(f, "Concurrently warm dentry and inode cache of PATH(s)\n\n");
    fprintf(f, "-j, --num-threads=N      Spawn N threads (default=%d)\n", opt_num_threads);
    fprintf(f, "-s, --init-queue-size=N  Init main queue and thread queues to N slots (default=%zu)\n", opt_init_queue_size);
    fprintf(f, "-a, --lock-free          Synchronize main queue with lock-free atomic (default=%c)\n", opt_lock_type == ATOMIC ? 'y' : 'n');
    fprintf(f, "-m, --lock-mutex         Synchronize main queue with mutex (default=%c)\n", opt_lock_type == MUTEX ? 'y' : 'n');
    fprintf(f, "-p, --print              Print paths to stdout\n");
    fprintf(f, "-h, --help               Show this help\n");
    fprintf(f, "-V, --version            Print program version\n");
    exit(exit_code);
}
