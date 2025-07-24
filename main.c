#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/inotify.h>
#include <pthread.h>

#define MAX_TASKS 1024
#define MAX_LINE 2048
#define TASK_FILE "tasks.txt"
#define LOG_DIR "./logs/"
#define EVENT_BUF_LEN (1024 * (sizeof(struct inotify_event) + 16))

typedef struct {
    time_t run_at;
    int priority;
    int retries;
    char *command;
} Task;

typedef struct {
    Task *tasks[MAX_TASKS];
    int size;
    pthread_mutex_t lock;
} TaskQueue;

TaskQueue queue = {.size = 0, .lock = PTHREAD_MUTEX_INITIALIZER};

// Comparator for priority queue
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
    snprintf(filename, sizeof(filename), LOG_DIR "task_%ld.log", time(NULL));
    FILE *f = fopen(filename, "a");
    if (f) {
        fprintf(f, "Command: %s\nOutput:\n%s\n", task->command, output);
        fclose(f);
    }
}

void *executor_thread(void *arg) {
    while (1) {
        Task *task = pop_task();
        if (!task) {
            sleep(1);
            continue;
        }

        time_t now = time(NULL);
        if (task->run_at > now) {
            push_task(task);
            sleep(1);
            continue;
        }

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
                free(task->command);
                free(task);
            } else {
                if (task->retries > 0) {
                    task->retries--;
                    task->run_at = time(NULL) + 5; // retry after delay
                    push_task(task);
                } else {
                    log_task_output(task, buffer);
                    free(task->command);
                    free(task);
                }
            }
        }
    }
    return NULL;
}

Task *parse_line(char *line) {
    Task *task = malloc(sizeof(Task));
    task->command = malloc(MAX_LINE);
    if (sscanf(line, "%ld %d %d %[^\n]", &task->run_at, &task->priority, &task->retries, task->command) != 4) {
        free(task->command);
        free(task);
        return NULL;
    }
    return task;
}

void load_tasks_from_file(const char *filename) {
    FILE *f = fopen(filename, "r");
    if (!f) return;
    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || strlen(line) < 2) continue;
        Task *task = parse_line(line);
        if (task) push_task(task);
    }
    fclose(f);
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
        load_tasks_from_file(TASK_FILE);
    }

    inotify_rm_watch(fd, wd);
    close(fd);
    return NULL;
}

int main() {
    mkdir(LOG_DIR, 0755);
    load_tasks_from_file(TASK_FILE);

    pthread_t exec_tid, watch_tid;
    pthread_create(&exec_tid, NULL, executor_thread, NULL);
    pthread_create(&watch_tid, NULL, watcher_thread, NULL);

    pthread_join(exec_tid, NULL);
    pthread_join(watch_tid, NULL);
    return 0;
}
