#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <event2/event.h>
#include <time.h>
#include <jansson.h>
#include <sqlite3.h>
#include <sys/wait.h>

#define DB_FILE "tasks.db"
#define MAX_CMD 512
#define MAX_BACKOFF 3600 // Max backoff 1 hour

#define MAX_FILENAME_LEN 255

#define SAVE_TASK_STATE_QUERY "UPDATE tasks SET " \
    "run_at=?, retries=?, backoff=?, enabled=? WHERE id=?"

typedef struct {
    int id;
    struct event *ev;
    struct event_base *base;
    time_t run_at;
    int interval;
    int retries;
    int max_retries;
    int backoff;
    int priority;
    int enabled;
    char *command;
} task_t;

sqlite3 *db;

// Comparator: Lower run_at first; if equal, higher priority first
int
task_cmp(const void *a, const void *b)
{
    task_t *t1 = *(task_t **)a, *t2 = *(task_t **)b;
    if (t1->run_at != t2->run_at) return (t1->run_at > t2->run_at) - (t1->run_at < t2->run_at);
    return t2->priority - t1->priority;
}

sqlite3*
init_db()
{
    if (sqlite3_open(DB_FILE, &db)) {
        fprintf(stderr, "DB error: %s\n", sqlite3_errmsg(db));
        exit(1);
    }

    const char *sql = "CREATE TABLE IF NOT EXISTS tasks ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "run_at INTEGER,"
        "interval INTEGER,"
        "retries INTEGER DEFAULT 0,"
        "max_retries INTEGER DEFAULT 3,"
        "backoff INTEGER DEFAULT 0,"
        "priority INTEGER DEFAULT 0,"
        "enabled INTEGER DEFAULT 1,"
        "command TEXT)";

    char *err = NULL;
    if (sqlite3_exec(db, sql, 0, 0, &err) != SQLITE_OK) {
        fprintf(stderr, "DB init error: %s\n", err);
        sqlite3_free(err);
        exit(1);
    }

    return db;
}

void
save_task_state(task_t *task)
{
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, SAVE_TASK_STATE_QUERY, -1, &stmt, NULL);
    sqlite3_bind_int64(stmt, 1, task->run_at);
    sqlite3_bind_int(stmt, 2, task->retries);
    sqlite3_bind_int(stmt, 3, task->backoff);
    sqlite3_bind_int(stmt, 4, task->enabled);
    sqlite3_bind_int(stmt, 5, task->id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void
save_task_to_db(sqlite3 *db, task_t *task)
{
    const char *sql = "INSERT INTO tasks (command, run_at, interval, priority, last_run) VALUES (?, ?, ?, ?, ?)";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, task->command, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, task->run_at);
    sqlite3_bind_int(stmt, 3, task->interval);
    sqlite3_bind_int(stmt, 4, task->priority);
    sqlite3_bind_int(stmt, 5, 0);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void
run_task(evutil_socket_t fd, short what, void *arg)
{
    task_t *task = (task_t *)arg;
    printf("[*] Running: %s\n", task->command);

    pid_t pid = fork();
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", task->command, NULL);
        exit(127);
    } else if (pid > 0) {
        int status;
        waitpid(pid, &status, 0);
    }

    if (task->interval > 0) {
        task->run_at = time(NULL) + task->interval;
        struct timeval tv = { task->interval, 0 };
        evtimer_add(task->ev, &tv);
    } else {
        event_free(task->ev);
        free(task->command);
        free(task);
    }
}

task_t*
schedule_task(struct event_base *base, sqlite3 *db, time_t run_at, int interval, int priority, const char *cmd)
{
    task_t *task = malloc(sizeof(task_t));
    task->base = base;
    task->run_at = run_at;
    task->interval = interval;
    task->priority = priority;
    task->command = strdup(cmd);
    task->ev = evtimer_new(base, run_task, task);

    time_t now = time(NULL);
    time_t delay = (run_at > now) ? (run_at - now) : 0;
    struct timeval tv = { delay, 0 };
    evtimer_add(task->ev, &tv);

    save_task_to_db(db, task);

    printf("[+] Scheduled: %s at %ld (interval %d, priority %d)\n", cmd, run_at, interval, priority);
    return task;
}

void
load_tasks_from_json(const char *filename, struct event_base *base, sqlite3 *db)
{
    json_error_t error;
    json_t *root = json_load_file(filename, 0, &error);
    if (!root || !json_is_array(root)) {
        fprintf(stderr, "Failed to load JSON: %s\n", error.text);
        exit(1);
    }

    size_t task_count = json_array_size(root);
    task_t **task_list = malloc(sizeof(task_t *) * task_count);

    for (size_t i = 0; i < task_count; i++) {
        json_t *item = json_array_get(root, i);
        const char *cmd = json_string_value(json_object_get(item, "command"));
        time_t run_at = (time_t)json_integer_value(json_object_get(item, "run_at"));
        int interval = json_integer_value(json_object_get(item, "interval"));
        int priority = json_integer_value(json_object_get(item, "priority"));

        task_t *task = malloc(sizeof(task_t));
        task->base = base;
        task->run_at = run_at;
        task->interval = interval;
        task->priority = priority;
        task->command = strdup(cmd);
        task_list[i] = task;
    }

    qsort(task_list, task_count, sizeof(task_t *), task_cmp);

    for (size_t i = 0; i < task_count; i++) {
        schedule_task(base, db, task_list[i]->run_at, task_list[i]->interval, task_list[i]->priority, task_list[i]->command);
        free(task_list[i]->command);
        free(task_list[i]);
    }

    free(task_list);
    json_decref(root);
}

int
main()
{
    struct event_base *base = event_base_new();
    db = init_db();

    load_tasks_from_json("tasks.json", base, db);

    printf("[*] Starting scheduler loop...\n");
    event_base_dispatch(base);

    sqlite3_close(db);
    event_base_free(base);

    return 0;
}
