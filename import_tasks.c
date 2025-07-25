#include <stdio.h>
#include <stdlib.h>

#include <jansson.h>
#include <sqlite3.h>

void
import_tasks(const char *json_file, const char *db_file)
{
    sqlite3 *db;
    sqlite3_open(db_file, &db);
    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    json_error_t error;
    json_t *root = json_load_file(json_file, 0, &error);
    if (!root || !json_is_array(root)) {
        fprintf(stderr, "Error loading JSON: %s\n", error.text);
        return;
    }

    for (size_t i = 0; i < json_array_size(root); i++) {
        json_t *task = json_array_get(root, i);
        const char *name = json_string_value(json_object_get(task, "name"));
        const char *command = json_string_value(json_object_get(task, "command"));
        int64_t run_at = json_integer_value(json_object_get(task, "run_at"));
        int interval = json_integer_value(json_object_get(task, "interval"));
        int priority = json_integer_value(json_object_get(task, "priority"));
        int enabled = json_is_true(json_object_get(task, "enabled")) ? 1 : 0;
        int max_retries = json_integer_value(json_object_get(task, "max_retries"));

        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO tasks "
            "(name, command, run_at, interval, priority, enabled, max_retries, next_run) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);", -1, &stmt, NULL);

        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, command, -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, run_at);
        sqlite3_bind_int(stmt, 4, interval);
        sqlite3_bind_int(stmt, 5, priority);
        sqlite3_bind_int(stmt, 6, enabled);
        sqlite3_bind_int(stmt, 7, max_retries);
        sqlite3_bind_int64(stmt, 8, run_at);

        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    sqlite3_close(db);
    json_decref(root);

    printf("Tasks imported successfully.\n");
}

int
main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <tasks.json> <tasks.db>\n", argv[0]);
        return 1;
    }

    import_tasks(argv[1], argv[2]);

    return 0;
}
