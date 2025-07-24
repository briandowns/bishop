#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sqlite3.h>
#include <jansson.h>

#define MAX_TASKS 4096
#define TASK_FILE "tasks.json"
#define DB_FILE "task.db"
#define LOG_DIR "./logs/"
#define EVENT_BUF_LEN (1024 * (sizeof(struct inotify_event) + 16))

typedef struct {
    int id;
    time_t run_at;
    int priority;
    int retries;
    int interval; // NEW: repeating interval in seconds
    char *command;
} Task;

typedef struct {
    Task *tasks[MAX_TASKS];
    int size;
    pthread_mutex_t lock;
} TaskQueue;

TaskQueue queue = {.size = 0, .lock = PTHREAD_MUTEX_INITIALIZER};
sqlite3 *db;

int task_cmp(Task *a, Task *b) {
    if (a->run_at != b->run_at)
        return a->run_at - b->run_at;
    return b->priority - a->priority;
}

void push_task(Task *task) {
    pthread_mutex_lock(&queue.lock);
    int i = queue.size++;
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (task_cmp(task, queue.tasks[parent]) >= 0) break;
        queue.tasks[i] = queue.tasks[parent];
        i = parent;
    }
    queue.tasks[i] = task;
    pthread_mutex_unlock(&queue.lock);
}

Task *pop_task() {
    pthread_mutex_lock(&queue.lock);
    if (queue.size == 0) {
        pthread_mutex_unlock(&queue.lock);
        return NULL;
    }
    Task *top = queue.tasks[0];
    Task *last = queue.tasks[--queue.size];
    int i = 0;
    while (i * 2 + 1 < queue.size) {
        int left = i * 2 + 1, right = i * 2 + 2;
        int smallest = left;
        if (right < queue.size && task_cmp(queue.tasks[right], queue.tasks[left]) < 0)
            smallest = right;
        if (task_cmp(last, queue.tasks[smallest]) <= 0) break;
        queue.tasks[i] = queue.tasks[smallest];
        i = smallest;
    }
    queue.tasks[i] = last;
    pthread_mutex_unlock(&queue.lock);

    return top;
}

void log_task_output(Task *task, const char *output) {
    char filename[256];
    snprintf(filename, sizeof(filename), LOG_DIR "task_%d_%ld.log", task->id, time(NULL));

    FILE *f = fopen(filename, "a");
    if (f) {
        fprintf(f, "Command: %s\nOutput:\n%s\n", task->command, output);
        fclose(f);
    }
}

void db_init() {
    sqlite3_open(DB_FILE, &db);
    char *err = NULL;

    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS tasks ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "run_at INTEGER, priority INTEGER, retries INTEGER,"
        "interval INTEGER, command TEXT"
        ");", NULL, NULL, &err);
}

void db_insert_task(Task *task) {
    printf("XXX - inserting task...\n");
    char *err = NULL;
    sqlite3_stmt *stmt;
    const char *sql = "INSERT INTO tasks (run_at, priority, retries, interval, command) VALUES (?, ?, ?, ?, ?)";

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) == SQLITE_OK) {
        printf("XXX - inserting into db - %s\n", task->command);
        sqlite3_bind_int64(stmt, 1, task->run_at);
        sqlite3_bind_int(stmt, 2, task->priority);
        sqlite3_bind_int(stmt, 3, task->retries);
        sqlite3_bind_int(stmt, 4, task->interval);
        sqlite3_bind_text(stmt, 5, task->command, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        task->id = (int)sqlite3_last_insert_rowid(db);
        sqlite3_finalize(stmt);
    }
}

void db_load_tasks() {
    sqlite3_stmt *stmt;
    const char *sql = "SELECT id, run_at, priority, retries, interval, command FROM tasks";

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            Task *t = malloc(sizeof(Task));
            t->id = sqlite3_column_int(stmt, 0);
            t->run_at = sqlite3_column_int64(stmt, 1);
            t->priority = sqlite3_column_int(stmt, 2);
            t->retries = sqlite3_column_int(stmt, 3);
            t->interval = sqlite3_column_int(stmt, 4);
            t->command = strdup((const char *)sqlite3_column_text(stmt, 5));
            printf("XXX - pushing to queue - %s\n", t->command);
            push_task(t);
        }
        sqlite3_finalize(stmt);
    }
}

void db_delete_task(int id) {
    char sql[128];
    snprintf(sql, sizeof(sql), "DELETE FROM tasks WHERE id = %d;", id);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
}

void *executor_thread(void *arg) {
    printf("XXX - running execution...\n");
    while (1) {
        Task *task = pop_task();
        if (!task) {
            printf("XXX - no task\n");
            sleep(1);
            continue;
        }

        if (task->interval < 1) {
            time_t now = time(NULL);
            if (task->run_at > now) {
                push_task(task);
                sleep(1);
                continue;
            }

            continue;
        }

        printf("XXX - proceeding with execution...\n");

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
            char buffer[4096];
            ssize_t count = read(pipefd[0], buffer, sizeof(buffer) - 1);
            buffer[count > 0 ? count : 0] = '\0';
            close(pipefd[0]);
            int status;
            waitpid(pid, &status, 0);

            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                log_task_output(task, buffer);

                if (task->interval > 0) {
                    task->run_at = time(NULL) + task->interval;
                    push_task(task);
                    // Keep in DB
                } else {
                    db_delete_task(task->id);
                    free(task->command);
                    free(task);
                }
            } else {
                if (task->retries > 0) {
                    task->retries--;
                    task->run_at = time(NULL) + 5;
                    push_task(task);
                } else {
                    log_task_output(task, buffer);

                    if (task->interval > 0) {
                        task->run_at = time(NULL) + task->interval;
                        task->retries = 3; // reset retries for repeats
                        push_task(task);
                    } else {
                        db_delete_task(task->id);
                        free(task->command);
                        free(task);
                    }
                }
            }
        }
    }
    return NULL;
}

void load_json_tasks(const char *filename) {
    json_error_t err;
    json_t *root = json_load_file(filename, 0, &err);
    if (!root || !json_is_array(root)) return;

    size_t i;
    json_t *item;
    json_array_foreach(root, i, item) {
        Task *t = malloc(sizeof(Task));
        t->id = -1;
        const char *cmd = NULL;
        json_unpack(item, "{s:i, s:i, s:i, s:i, s:s}",
            "run_at", &t->run_at,
            "priority", &t->priority,
            "retries", &t->retries,
            "interval", &t->interval,
            "command", &cmd);

        t->command = strdup(cmd);
        db_insert_task(t);
        push_task(t);
    }

    json_decref(root);
}

void *watcher_thread(void *arg) {
    int fd = inotify_init();
    if (fd < 0) return NULL;

    int wd = inotify_add_watch(fd, TASK_FILE, IN_MODIFY);
    char buffer[EVENT_BUF_LEN];

    while (1) {
        int length = read(fd, buffer, EVENT_BUF_LEN);
        if (length < 0) continue;
        sleep(1); // debounce
        load_json_tasks(TASK_FILE);
    }

    inotify_rm_watch(fd, wd);
    close(fd);
    return NULL;
}

int main() {
    printf("XXX - creating dir...\n");
    mkdir(LOG_DIR, 0755);

    printf("XXX - init db...\n");
    db_init();

    printf("XXX - loading db...\n");
    db_load_tasks();

    pthread_t exec_tid, watch_tid;
    pthread_create(&exec_tid, NULL, executor_thread, NULL);
    pthread_create(&watch_tid, NULL, watcher_thread, NULL);

    pthread_join(exec_tid, NULL);
    pthread_join(watch_tid, NULL);
    sqlite3_close(db);
    return 0;
}
