// Required libraries:
// sudo apt install libevent-dev libjansson-dev libsqlite3-dev

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <event2/event.h>
#include <jansson.h>
#include <sqlite3.h>

#define DB_FILE "tasks.db"
#define MAX_CMD 512
#define MAX_BACKOFF 3600 // Max backoff 1 hour

struct Task {
    int id;
    time_t run_at;
    int interval;
    int retries;
    int max_retries;
    int backoff;
    int priority;
    int enabled;
    char command[MAX_CMD];
    struct event *ev;
};

sqlite3 *db;
struct event_base *base;

void
log_task_output(int id, const char *output)
{
    char filename[64];
    snprintf(filename, sizeof(filename), "task_%d.log", id);
    FILE *fp = fopen(filename, "a");
    if (fp) {
        fprintf(fp, "%s\n", output);
        fclose(fp);
    }
}

void
save_task_state(struct Task *task)
{
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "UPDATE tasks SET run_at=?, retries=?, backoff=?, enabled=? WHERE id=?", -1, &stmt, NULL);
    sqlite3_bind_int64(stmt, 1, task->run_at);
    sqlite3_bind_int(stmt, 2, task->retries);
    sqlite3_bind_int(stmt, 3, task->backoff);
    sqlite3_bind_int(stmt, 4, task->enabled);
    sqlite3_bind_int(stmt, 5, task->id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void
run_task(evutil_socket_t fd, short what, void *arg)
{
    struct Task *task = (struct Task *)arg;
    if (!task->enabled) return;

    printf("[*] Running task %d: %s\n", task->id, task->command);
    int pipefd[2];
    pipe(pipefd);
    pid_t pid = fork();
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        execl("/bin/sh", "sh", "-c", task->command, NULL);
        exit(127);
    } else {
        close(pipefd[1]);
        char buffer[1024];
        ssize_t count;
        FILE *fp = fdopen(pipefd[0], "r");
        while (fgets(buffer, sizeof(buffer), fp)) {
            log_task_output(task->id, buffer);
        }
        fclose(fp);
        int status;
        waitpid(pid, &status, 0);
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            task->retries = 0;
            task->backoff = 0;
            if (task->interval > 0) {
                task->run_at = time(NULL) + task->interval;
            } else {
                task->enabled = 0;
            }
        } else {
            task->retries++;
            if (task->retries > task->max_retries) {
                printf("[!] Task %d failed permanently\n", task->id);
                task->enabled = 0;
            } else {
                task->backoff = (task->backoff == 0) ? 5 : task->backoff * 2;
                if (task->backoff > MAX_BACKOFF) task->backoff = MAX_BACKOFF;
                task->run_at = time(NULL) + task->backoff;
                printf("[!] Task %d failed. Retrying in %d sec\n", task->id, task->backoff);
            }
        }
        save_task_state(task);
        struct timeval tv = { task->run_at - time(NULL), 0 };
        evtimer_add(task->ev, &tv);
    }
}

void
schedule_task(struct Task *task)
{
    time_t now = time(NULL);
    time_t delay = (task->run_at > now) ? (task->run_at - now) : 0;
    struct timeval tv = { delay, 0 };
    task->ev = evtimer_new(base, run_task, task);
    evtimer_add(task->ev, &tv);
    printf("[+] Scheduled task %d in %ld sec\n", task->id, delay);
}

void
load_tasks_from_db()
{
    sqlite3_stmt *stmt;
    const char *sql = "SELECT id, run_at, interval, retries, max_retries, backoff, priority, enabled, command FROM tasks WHERE enabled=1";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        struct Task *task = malloc(sizeof(struct Task));
        task->id = sqlite3_column_int(stmt, 0);
        task->run_at = sqlite3_column_int64(stmt, 1);
        task->interval = sqlite3_column_int(stmt, 2);
        task->retries = sqlite3_column_int(stmt, 3);
        task->max_retries = sqlite3_column_int(stmt, 4);
        task->backoff = sqlite3_column_int(stmt, 5);
        task->priority = sqlite3_column_int(stmt, 6);
        task->enabled = sqlite3_column_int(stmt, 7);
        strncpy(task->command, (const char *)sqlite3_column_text(stmt, 8), MAX_CMD);
        schedule_task(task);
    }
    sqlite3_finalize(stmt);
}

void
create_db_if_needed()
{
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
    sqlite3_exec(db, sql, 0, 0, &err);
    if (err) {
        fprintf(stderr, "DB error: %s\n", err);
        sqlite3_free(err);
    }
}

void
import_tasks_from_json(const char *filename)
{
    json_error_t error;
    json_t *root = json_load_file(filename, 0, &error);
    if (!root) {
        fprintf(stderr, "JSON error: %s\n", error.text);
        return;
    }
    size_t i;
    json_t *task;
    sqlite3_stmt *stmt;
    json_array_foreach(root, i, task) {
        const char *cmd = json_string_value(json_object_get(task, "command"));
        time_t run_at = json_integer_value(json_object_get(task, "run_at"));
        int interval = json_integer_value(json_object_get(task, "interval"));
        int retries = json_integer_value(json_object_get(task, "retries"));
        int max_retries = json_integer_value(json_object_get(task, "max_retries"));
        int backoff = json_integer_value(json_object_get(task, "backoff"));
        int priority = json_integer_value(json_object_get(task, "priority"));
        int enabled = json_boolean_value(json_object_get(task, "enabled"));

        sqlite3_prepare_v2(db, "INSERT INTO tasks (run_at, interval, retries, max_retries, backoff, priority, enabled, command) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", -1, &stmt, NULL);
        sqlite3_bind_int64(stmt, 1, run_at);
        sqlite3_bind_int(stmt, 2, interval);
        sqlite3_bind_int(stmt, 3, retries);
        sqlite3_bind_int(stmt, 4, max_retries);
        sqlite3_bind_int(stmt, 5, backoff);
        sqlite3_bind_int(stmt, 6, priority);
        sqlite3_bind_int(stmt, 7, enabled);
        sqlite3_bind_text(stmt, 8, cmd, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    json_decref(root);
}

int
main(int argc, char *argv[])
{
    if (sqlite3_open(DB_FILE, &db)) {
        fprintf(stderr, "Can't open DB\n");
        return 1;
    }
    create_db_if_needed();

    if (argc == 2) {
        import_tasks_from_json(argv[1]);
    }

    base = event_base_new();
    load_tasks_from_db();
    event_base_dispatch(base);
    event_base_free(base);
    sqlite3_close(db);
    return 0;
}
