#ifdef MAIN_SIMULATION
#define QUAKEY_ENABLE_MOCKS

#include <signal.h>
#include <stdint.h>
#include <string.h>
#include <quakey.h>

#include "node.h"
#include "client.h"

static volatile int simulation_running = 1;

static void sigint_handler(int sig)
{
    (void)sig;
    simulation_running = 0;
}

int main(void)
{
    signal(SIGINT, sigint_handler);

    Quakey *quakey;
    int ret = quakey_init(&quakey, 2);
    if (ret < 0)
        return -1;

    // Client 1
    {
        QuakeySpawn config = {
            .name       = "rndcli1",
            .state_size = sizeof(ClientState),
            .init_func  = client_init,
            .tick_func  = client_tick,
            .free_func  = client_free,
            .addrs      = (char*[]) { "127.0.0.2" },
            .num_addrs  = 1,
            .disk_size  = 10<<20,
            .platform   = QUAKEY_LINUX,
        };
        quakey_spawn(quakey, config, "cli --server 127.0.0.4:8080 --server 127.0.0.5:8080 --server 127.0.0.6:8080");
    }

    // Client 2
    {
        QuakeySpawn config = {
            .name       = "rndcli2",
            .state_size = sizeof(ClientState),
            .init_func  = client_init,
            .tick_func  = client_tick,
            .free_func  = client_free,
            .addrs      = (char*[]) { "127.0.0.3" },
            .num_addrs  = 1,
            .disk_size  = 10<<20,
            .platform   = QUAKEY_LINUX,
        };
        quakey_spawn(quakey, config, "cli --server 127.0.0.4:8080 --server 127.0.0.5:8080 --server 127.0.0.6:8080");
    }

    // Node 1
    {
        QuakeySpawn config = {
            .name       = "node1",
            .state_size = sizeof(NodeState),
            .init_func  = node_init,
            .tick_func  = node_tick,
            .free_func  = node_free,
            .addrs      = (char*[]) { "127.0.0.4" },
            .num_addrs  = 1,
            .disk_size  = 10<<20,
            .platform   = QUAKEY_LINUX,
        };
        quakey_spawn(quakey, config, "nd --addr 127.0.0.4:8080 --peer 127.0.0.5:8080 --peer 127.0.0.6:8080");
    }

    // Node 2
    {
        QuakeySpawn config = {
            .name       = "node2",
            .state_size = sizeof(NodeState),
            .init_func  = node_init,
            .tick_func  = node_tick,
            .free_func  = node_free,
            .addrs      = (char*[]) { "127.0.0.5" },
            .num_addrs  = 1,
            .disk_size  = 10<<20,
            .platform   = QUAKEY_LINUX,
        };
        quakey_spawn(quakey, config, "nd --peer 127.0.0.4:8080 --addr 127.0.0.5:8080 --peer 127.0.0.6:8080");
    }

    // Node 3
    {
        QuakeySpawn config = {
            .name       = "node3",
            .state_size = sizeof(NodeState),
            .init_func  = node_init,
            .tick_func  = node_tick,
            .free_func  = node_free,
            .addrs      = (char*[]) { "127.0.0.6" },
            .num_addrs  = 1,
            .disk_size  = 10<<20,
            .platform   = QUAKEY_LINUX,
        };
        quakey_spawn(quakey, config, "nd --peer 127.0.0.4:8080 --peer 127.0.0.5:8080 --addr 127.0.0.6:8080");
    }

    while (simulation_running)
        quakey_schedule_one(quakey);

    quakey_free(quakey);
    return 0;
}

#endif // MAIN_SIMULATION
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
#ifdef MAIN_CLIENT

#include <poll.h>

#include "client.h"

#define POLL_CAPACITY 1024

int main(int argc, char **argv)
{
    int ret;
    ClientState state;

    void*         poll_ctxs[POLL_CAPACITY];
    struct pollfd poll_array[POLL_CAPACITY];
    int poll_count;
    int poll_timeout;

    ret = client_init(
        &state,
        argc,
        argv,
        poll_ctxs,
        poll_array,
        POLL_CAPACITY,
        &poll_count,
        &poll_timeout
    );
    if (ret < 0)
        return -1;

    for (;;) {

#ifdef _WIN32
        WSAPoll(poll_array, poll_count, poll_timeout);
#else
        poll(poll_array, poll_count, poll_timeout);
#endif

        ret = client_tick(
            &state,
            poll_ctxs,
            poll_array,
            POLL_CAPACITY,
            &poll_count,
            &poll_timeout
        );
        if (ret < 0)
            return -1;
    }

    client_free(&state);
    return 0;
}

#endif // MAIN_CLIENT
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
#ifdef MAIN_SERVER

#include <poll.h>

#include "node.h"

#define POLL_CAPACITY 1024

int main(int argc, char **argv)
{
    int ret;
    NodeState state;

    void*         poll_ctxs[POLL_CAPACITY];
    struct pollfd poll_array[POLL_CAPACITY];
    int poll_count;
    int poll_timeout;

    ret = node_init(
        &state,
        argc,
        argv,
        poll_ctxs,
        poll_array,
        POLL_CAPACITY,
        &poll_count,
        &poll_timeout
    );
    if (ret < 0)
        return -1;

    for (;;) {

#ifdef _WIN32
        WSAPoll(poll_array, poll_count, poll_timeout);
#else
        poll(poll_array, poll_count, poll_timeout);
#endif

        ret = node_tick(
            &state,
            poll_ctxs,
            poll_array,
            POLL_CAPACITY,
            &poll_count,
            &poll_timeout
        );
        if (ret < 0)
            return -1;
    }

    node_free(&state);
    return 0;
}

#endif // MAIN_SERVER